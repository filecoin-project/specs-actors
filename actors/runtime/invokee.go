package runtime

type Invokee interface {
	Exports() []interface{}
}
