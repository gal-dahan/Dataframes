package table

type TableReader interface {
	Read() ([][]string, error)
}
