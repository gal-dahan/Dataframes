package operation

import (
	"math"
	"strconv"

	"github.com/gal-dahan/Dataframes/pipeline"
)

func FilterRows(condition func([]string) bool) pipeline.Operation {
	return pipeline.OperationFunc(func(in <-chan []string, out chan<- []string) {
		defer close(out)
		for row := range in {
			if condition(row) {
				out <- row
			}
		}
	})
}

func GetColumn(index int) pipeline.Operation {
	return pipeline.OperationFunc(func(in <-chan []string, out chan<- []string) {
		defer close(out)
		for row := range in {
			if index >= 0 && index < len(row) {
				out <- []string{row[index]}
			}
		}
	})
}

func Avg() pipeline.Operation {
	return pipeline.OperationFunc(func(in <-chan []string, out chan<- []string) {
		defer close(out)
		var sum float64
		count := 0
		for row := range in {
			for _, val := range row {
				num, err := strconv.ParseFloat(val, 64)
				if err == nil {
					sum += num
					count++
				}
			}
		}
		if count > 0 {
			avg := sum / float64(count)
			out <- []string{strconv.FormatFloat(avg, 'f', -1, 64)}
		}
	})
}

func Ceil() pipeline.Operation {
	return pipeline.OperationFunc(func(in <-chan []string, out chan<- []string) {
		defer close(out)
		for row := range in {
			var newRow []string
			for _, val := range row {
				num, err := strconv.ParseFloat(val, 64)
				if err == nil {
					ceilValue := math.Ceil(num)
					newRow = append(newRow, strconv.FormatFloat(ceilValue, 'f', -1, 64))
				}
			}
			out <- newRow
		}
	})
}

func ForEveryColumn() pipeline.Operation {
	return pipeline.OperationFunc(func(in <-chan []string, out chan<- []string) {
		defer close(out)
		for row := range in {
			for j, cell := range row {
				num, err := strconv.Atoi(cell)
				if err == nil {
					row[j] = strconv.Itoa(num * 2)
				}
			}
			out <- row
		}
	})
}

func GetColumns(indices ...int) pipeline.Operation {
	return pipeline.OperationFunc(func(in <-chan []string, out chan<- []string) {
		defer close(out)
		for row := range in {
			var newRow []string
			for _, idx := range indices {
				if idx >= 0 && idx < len(row) {
					newRow = append(newRow, row[idx])
				}
			}
			out <- newRow
		}
	})
}

func GetRows(indices ...int) pipeline.Operation {
	return pipeline.OperationFunc(func(in <-chan []string, out chan<- []string) {
		defer close(out)
		for idx := range indices {
			if idx >= 0 {
				for i := 0; i <= idx; i++ {
					row := <-in
					if i == idx {
						out <- row
					}
				}
			}
		}
	})
}

func SumRow() pipeline.Operation {
	return pipeline.OperationFunc(func(in <-chan []string, out chan<- []string) {
		defer close(out)
		for row := range in {
			sum := 0
			for _, cell := range row {
				num, err := strconv.Atoi(cell)
				if err == nil {
					sum += num
				}
			}
			out <- []string{strconv.Itoa(sum)}
		}
	})
}
