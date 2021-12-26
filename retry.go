package bigquery

import (
	"google.golang.org/api/googleapi"
	"math/rand"
	"net/http"
	"time"
)

func shallRetry(err error) bool {
	if apiError, ok := err.(*googleapi.Error); ok {
		switch apiError.Code {
		case http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusBadGateway:
			return true
		}
	}
	return false
}

//retrier represents abstraction holding sleep duration between retries (back-off)
type retrier struct {
	Count      int
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	duration   time.Duration
}

// Pause returns the next time.Duration that the caller should use to backoff.
func (b *retrier) Pause() time.Duration {
	if b.Initial == 0 {
		b.Initial = time.Second
	}
	if b.duration == 0 {
		b.duration = b.Initial
	}
	if b.Max == 0 {
		b.Max = 30 * time.Second
	}
	if b.Multiplier < 1 {
		b.Multiplier = 2
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	result := time.Duration(1 + rnd.Int63n(int64(b.duration)))
	b.duration = time.Duration(float64(b.duration) * b.Multiplier)
	if b.duration > b.Max {
		b.duration = b.Max
	}
	return result
}

//newRetries creates a retrier
func newRetries() *retrier {
	return &retrier{}
}

//runWithRetries run with retries
func runWithRetries(f func() error, maxRetries int) (err error) {
	aRetrier := newRetries()
	for i := 0; i < maxRetries; i++ {
		err = f()
		if !shallRetry(err) {
			return err
		}
		time.Sleep(aRetrier.Pause())
	}
	return err
}
