package ingestion

type Service struct {
}

func (s *Service) Ingest(SQL string) error {
	return nil
}

func (s *Service) Load(ingestion *Ingestion) error {
	return nil
}

func (s *Service) Stream(ingestion *Ingestion) error {
	return nil
}
