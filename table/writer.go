package table

type TableWriter interface {
	Write([][]string) error
}