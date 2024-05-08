package table

type TableReader interface {
	Read() (<-chan []string, error)
}
