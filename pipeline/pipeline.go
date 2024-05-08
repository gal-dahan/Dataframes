package pipeline

import (
	"encoding/csv"
	"io"
	"log"
	"os"

	"github.com/gal-dahan/Dataframes/table"
)

type Operation interface {
	Apply(in <-chan []string, out chan<- []string)
}

type Pipeline struct {
	reader table.TableReader
	ops    []Operation
}

type OperationFunc func(in <-chan []string, out chan<- []string)

func (f OperationFunc) Apply(in <-chan []string, out chan<- []string) {
	f(in, out)
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

func (r *CSVTableReader) Read() (<-chan []string, error) {
	file, err := os.Open(r.path)
	if err != nil {
		return nil, err
	}

	csvReader := csv.NewReader(file)

	out := make(chan []string)
	go func() {
		defer close(out)
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Println("Error reading CSV:", err)
				continue
			}
			out <- record
		}
	}()
	return out, nil
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
	if err != nil {
		log.Fatal(err)
	}

	for _, op := range p.ops {
		out := make(chan []string)
		go op.Apply(rows, out)
		rows = out
	}

	for row := range rows {
		if err := csvWriter.Write(row); err != nil {
			log.Println("Error writing row to CSV:", err)
		}
	}
}
