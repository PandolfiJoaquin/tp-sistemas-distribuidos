package models

import "strconv"

type TotalQueryResults struct {
	QueryId int
	Items   []QueryResult
}

type QueryResult interface {
	QueryId() int
	String() string
}

type Q1Movie struct {
	Title  string   `json:"title"`
	Genres []Genre `json:"genres"`
}

func (q Q1Movie) String() string {
	str := "Query 1: " + q.Title + " | Genres: ["
	for i := 0; i < len(q.Genres); i++ {
		str += q.Genres[i].Name
		if i < len(q.Genres)-1 {
			str += ", "
		}
	}
	str += "]"
	return str
}

type Q2Country struct {
	Country    Country `json:"country"`
}

// placeholder for Q2Country
func (q Q2Country) String() string {
	return "Query 2: " + q.Country.Name
}

type Q3Movie struct {
	Title string `json:"title"`
}

// placeholder for Q3Movie
func (q Q3Movie) String() string {
	return "Query 3: " + q.Title
}

type Q4Actors struct {
	Actor       string `json:"actor"`
	Appearances uint32 `json:"appearances"`
}

// placeholder for Q4Actors
func (q Q4Actors) String() string {
	return "Query 4: " + q.Actor + " | Appearances: " + strconv.Itoa(int(q.Appearances))
}

type Q5Avg struct {
	Avg float32 `json:"avg"`
}

// placeholder for Q5Avg
func (q Q5Avg) String() string {
	return "Query 5: Average Rating: " + strconv.FormatFloat(float64(q.Avg), 'f', 2, 32)
}

func (q Q1Movie) QueryId() int   { return 1 }
func (q Q2Country) QueryId() int { return 2 }
func (q Q3Movie) QueryId() int   { return 3 }
func (q Q4Actors) QueryId() int  { return 4 }
func (q Q5Avg) QueryId() int     { return 5 }
