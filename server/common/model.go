package common

type Movie struct {
	ID                  string   `json:"id"`
	Title               string   `json:"title"`
	Year                int      `json:"year"`
	Genre               string   `json:"genre"`
	ProductionCountries []string `json:"production_countries"`
}

type Header struct {
	Weight uint32 `json:"weight"`
}

type Batch struct {
	Header Header  `json:"header"`
	Movies []Movie `json:"movies"`
}

var mockedMovies = []Movie{
	{
		ID:                  "1",
		Title:               "Interstellar",
		Year:                2010,
		Genre:               "Space",
		ProductionCountries: []string{"England", "USA"},
	},
	{
		ID:                  "2",
		Title:               "The Dark Knight",
		Year:                2008,
		Genre:               "Action",
		ProductionCountries: []string{"USA"},
	},
	{
		ID:                  "3",
		Title:               "Rata blanca",
		Year:                2011,
		Genre:               "Comedy",
		ProductionCountries: []string{"Argentina"},
	},
	{
		ID:                  "4",
		Title:               "El padrino",
		Year:                1980,
		Genre:               "Drama",
		ProductionCountries: []string{"España"},
	},
	{
		ID:                  "5",
		Title:               "El secreto de sus ojos",
		Year:                2009,
		Genre:               "Drama",
		ProductionCountries: []string{"Argentina", "España"},
	},
}
var MockedBatch = Batch{
	Header: Header{Weight: uint32(len(mockedMovies))},
	Movies: mockedMovies,
}
