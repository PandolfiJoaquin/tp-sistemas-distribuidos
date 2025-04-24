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
	Title  string  `json:"title"`
	Genres []Genre `json:"genres"`
}

func (q Q1Movie) String() string {
	str := q.Title + " | Genres: ["
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
	Country Country `json:"country"`
	Budget  uint64  `json:"budget"`
}

// placeholder for Q2Country
func (q Q2Country) String() string {
	return q.Country.Name + " | Budget: " + strconv.FormatUint(q.Budget, 10)
}

type Q3Result struct {
	Best  Q3Movie `json:"best"`
	Worst Q3Movie `json:"worst"`
}

type Q3Movie struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	// rating float32 `json:"rating"`
}

// placeholder for Q3Movie
func (q Q3Result) String() string {
	return "Query 3: Best Movie: " + q.Best.Title + " | Worst Movie: " + q.Worst.Title
}

type Q4Actors struct {
	ActorName   string `json:"actor"`
	ActorId     string `json:"actor_id"`
	Appearances uint32 `json:"appearances"`
}

// placeholder for Q4Actors
func (q Q4Actors) String() string {
	return "Actor: " + q.ActorName + " | Appearances: " + strconv.Itoa(int(q.Appearances))
}

type Q5Avg struct {
	PositiveAvgProfitRatio float64 `json:"positive_avg_profit_ratio"`
	NegativeAvgProfitRatio float64 `json:"negative_avg_profit_ratio"`
}

// placeholder for Q5Avg
func (q Q5Avg) String() string {
	return "Positive Avg Profit Ratio: " + strconv.FormatFloat(q.PositiveAvgProfitRatio, 'f', 2, 32) + " | Negative Avg Profit Ratio: " + strconv.FormatFloat(q.NegativeAvgProfitRatio, 'f', 2, 32)
}

func (q Q1Movie) QueryId() int   { return 1 }
func (q Q2Country) QueryId() int { return 2 }
func (q Q3Result) QueryId() int  { return 3 }
func (q Q4Actors) QueryId() int  { return 4 }
func (q Q5Avg) QueryId() int     { return 5 }
