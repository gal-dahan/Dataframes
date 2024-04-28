package main

import (
	"log"
	"github.com/gal-dahan/Dataframes/operation"
	"github.com/gal-dahan/Dataframes/pipeline"
)

func main() {

    // Example 1
    pipeline1 := pipeline.Read("input-example-1.csv").
        With(operation.FilterRows(func(record []string) bool {
            return record[3] == "Iowa" 
        })).
        With(operation.GetColumn(1)). 
        With(operation.Avg()).       
        With(operation.Ceil())       

    pipeline1Output := "output-example-1.csv"
    pipeline1.Write(pipeline1Output)
    log.Printf("Pipeline 1 processing complete. Results written to %s", pipeline1Output)
    log.Printf("*******************************************************************")

    // Example 2
    pipeline2 := pipeline.Read("input-example-2.csv").
        With(operation.ForEveryColumn()).        
        With(operation.GetColumns(2, 4, 6)).     
        With(operation.GetRows(1, 3, 5)).      
        With(operation.SumRow())                

    pipeline2Output := "output-example-2.csv"
    pipeline2.Write(pipeline2Output)
    log.Printf("Pipeline 2 processing complete. Results written to %s", pipeline2Output)

}
