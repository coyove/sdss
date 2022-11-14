package types

type DocumentToken struct {
	Namespace string
	Id        string
	Token     string
	OutError  chan error
}
