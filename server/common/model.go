package common

type Movie struct {
	ID                  string   `json:"id"`
	Title               string   `json:"title"`
	Year                int      `json:"year"`
	Genre               string   `json:"genre"`
	ProductionCountries []string `json:"production_countries"`
}
