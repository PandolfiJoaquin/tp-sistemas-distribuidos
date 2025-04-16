package main

import (
	"analyzer-client/utils"
	"fmt"
)

const FILEPATH = "movies_metadata.csv"



func main() {
	movies := make([]*utils.Movie, 0)
	reader := utils.NewMoviesReader(FILEPATH, 1000)
	defer reader.Close()
	for reader.Finished == false {
		movie, err := reader.ReadMovie()
		if err != nil {
			fmt.Println("Error reading movie:", err)
			return
		}
		if movie == nil {
			continue
		}

		movies = append(movies, movie)

	} 

	fmt.Printf("Read %d movies\n", len(movies))


	

}
