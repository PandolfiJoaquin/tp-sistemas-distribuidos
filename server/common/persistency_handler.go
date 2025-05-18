package common

type PersistencyHandler struct {
	basePath string
}

func NewPersistencyHandler(basePath string) *PersistencyHandler {
	return &PersistencyHandler{basePath: basePath}
}

func (p *PersistencyHandler) Save() error {
	return nil
}
