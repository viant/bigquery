package reader

import (
	"fmt"
	"io"
	"sync"
)

var readers = newRegistry()

func Register(ID string, aReader io.Reader) error {
	return readers.add(ID, aReader)
}

func Unregister(ID string) {
	readers.remove(ID)
}

type reader struct {
	io.Reader
	unregister func()
}

func (r *reader) Read(b []byte) (int, error) {
	n, err := r.Read(b)
	if err == io.EOF {
		r.unregister()
	}
	return n, err
}

type registry struct {
	mux     sync.Mutex
	readers map[string]*reader
}

func newRegistry() *registry {
	return &registry{readers: map[string]*reader{}}
}

func (r *registry) add(ID string, aReader io.Reader) error {
	r.mux.Lock()
	defer r.mux.Unlock()
	if _, ok := r.readers[ID]; ok {
		return fmt.Errorf("reader: %v, had been already registred", ID)
	}
	r.readers[ID] = &reader{
		Reader: aReader,
		unregister: func() {
			r.remove(ID)
		},
	}
	return nil
}

func (r *registry) remove(ID string) {
	r.mux.Lock()
	defer r.mux.Unlock()
	delete(r.readers, ID)
}
