package operation

import (
	"github.com/gal-dahan/Dataframes/pipeline"
	"strconv"
	"math"
)

func FilterRows(condition func([]string) bool) pipeline.Operation {
	return pipeline.OperationFunc(func(rows [][]string) [][]string {
		var filteredRows [][]string
		for _, row := range rows {
			if condition(row) {
				filteredRows = append(filteredRows, row)
			}
		}
		return filteredRows
	})
}

func GetColumn(index int) pipeline.Operation {
	return pipeline.OperationFunc(func(rows [][]string) [][]string {
		var column []string
		for _, row := range rows {
			if index >= 0 && index < len(row) {
				column = append(column, row[index])
			}
		}
		return [][]string{column}
	})
}

func Avg() pipeline.Operation {
	return pipeline.OperationFunc(func(rows [][]string) [][]string {
		var avgs [][]string
		for _, row := range rows {
			if len(row) == 0 {
				continue
			}
			var sum float64
			count := 0
			for _, val := range row {
				num, err := strconv.ParseFloat(val, 64)
				if err == nil {
					sum += num
					count++
				}
			}
			if count > 0 {
				avg := sum / float64(count)
				avgs = append(avgs, []string{strconv.FormatFloat(avg, 'f', -1, 64)})
			}
		}
		return avgs
	})
}

func Ceil() pipeline.Operation {
	return pipeline.OperationFunc(func(rows [][]string) [][]string {
		var ceiled [][]string
		for _, row := range rows {
			var newRow []string
			for _, val := range row {
				num, err := strconv.ParseFloat(val, 64)
				if err == nil {
					ceilValue := math.Ceil(num)
					newRow = append(newRow, strconv.FormatFloat(ceilValue, 'f', -1, 64))
				}
			}
			ceiled = append(ceiled, newRow)
		}
		return ceiled
	})
}
func ForEveryColumn() pipeline.Operation {
	return pipeline.OperationFunc(func(rows [][]string) [][]string {
		for i, row := range rows {
			for j, cell := range row {
				num, err := strconv.Atoi(cell)
				if err == nil {
					rows[i][j] = strconv.Itoa(num * 2)
				}
			}
		}
		return rows
	})
}

func GetColumns(indices ...int) pipeline.Operation {
	return pipeline.OperationFunc(func(rows [][]string) [][]string {
		var selected [][]string
		for _, row := range rows {
			var newRow []string
			for _, idx := range indices {
				if idx >= 0 && idx < len(row) {
					newRow = append(newRow, row[idx])
				}
			}
			selected = append(selected, newRow)
		}
		return selected
	})
}

func GetRows(indices ...int) pipeline.Operation {
	return pipeline.OperationFunc(func(rows [][]string) [][]string {
		var selected [][]string
		for _, idx := range indices {
			if idx >= 0 && idx < len(rows) {
				selected = append(selected, rows[idx])
			}
		}
		return selected
	})
}

func SumRow() pipeline.Operation {
	return pipeline.OperationFunc(func(rows [][]string) [][]string {
		var summed [][]string
		for _, row := range rows {
			sum := 0
			for _, cell := range row {
				num, err := strconv.Atoi(cell)
				if err == nil {
					sum += num
				}
			}
			summed = append(summed, []string{strconv.Itoa(sum)})
		}
		return summed
	})
}
