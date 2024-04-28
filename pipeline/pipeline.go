package pipeline
import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"github.com/gal-dahan/Dataframes/table"
)

type Operation interface {
	Apply([][]string) [][]string
}

type Pipeline struct {
	reader table.TableReader
	ops    []Operation
}
type OperationFunc func([][]string) [][]string

func (f OperationFunc) Apply(rows [][]string) [][]string {
	return f(rows)
}
func (p *Pipeline) Read(reader table.TableReader) *Pipeline {
	p.reader = reader
	return p
}

func (p *Pipeline) With(op Operation) *Pipeline {
	p.ops = append(p.ops, op)
	return p
}

func Read(path string) *Pipeline {
	return &Pipeline{reader: &CSVTableReader{path: path}}
}
type CSVTableReader struct {
	path string
}

func (r *CSVTableReader) Read() ([][]string, error) {
	file, err := os.Open(r.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (p *Pipeline) Write(path string) {
	file, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	csvWriter := csv.NewWriter(file)
	defer csvWriter.Flush()

	rows, err := p.reader.Read()
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}

	for _, op := range p.ops {
		rows = op.Apply(rows)
	}

	if err := csvWriter.WriteAll(rows); err != nil {
		log.Fatal(err)
	}
}