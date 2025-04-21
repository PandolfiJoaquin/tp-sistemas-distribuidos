package main

import (
	"fmt"
	"log/slog"
	"pkg/log"
)

const (
	server       = "gateway:12345"
	MoviesBatch  = 30
	ReviewsBatch = 300
	CreditsBatch = 10
)

func main() {
	logger, err := log.SetupLogger("client", false, nil)
	if err != nil {
		fmt.Printf("error creating logger: %v", err)
		return
	}
	slog.SetDefault(logger)

	config := NewClientConfig(server, MoviesBatch, ReviewsBatch, CreditsBatch)
	client := NewClient(config)

	slog.Info("client created successfully")

	client.Start()

	// Open the CSV file
	//file, err := os.Open("credits.csv")
	//if err != nil {
	//	fmt.Printf("error opening file: %v\n", err)
	//	return
	//}
	//
	//scanner := bufio.NewScanner(file)
	//scanner.Split(bufio.ScanLines)
	//scanner.Scan()
	//line := scanner.Text()
	//
	//fmt.Printf("input: %s\n", line)
	//
	//_, err = utils.FixJSONObject(line)
	//if err != nil {
	//	panic(err)
	//}

	//reader, _ := utils.NewMoviesReader("movies_metadata.csv", 1)
	//
	//for !reader.Finished {
	//	movie, err := reader.ReadMovies()
	//	//fmt.Printf("Total read movies: %d\n", reader.Total)
	//	if err != nil {
	//		fmt.Printf("Read movie: %s\n", movie)
	//		if err.Error() == "EOF" {
	//			break
	//		}
	//		slog.Error("error reading movies", slog.String("error", err.Error()))
	//		return
	//	}
	//	fmt.Printf("Movies Read: %s\n", reader.Total)
	//}
	//
	//readerC, _ := utils.NewCreditsReader("credits.csv", 1)
	//
	//for !readerC.Finished {
	//	movie, err := readerC.ReadCredits()
	//	//fmt.Printf("Total read movies: %d\n", reader.Total)
	//	if err != nil {
	//		fmt.Printf("Read Credits: %s\n", movie)
	//		if err.Error() == "EOF" {
	//			break
	//		}
	//		slog.Error("error reading Credits", slog.String("error", err.Error()))
	//		return
	//	}
	//	fmt.Printf("Read credits: %s\n", readerC.Total)
	//}
}
