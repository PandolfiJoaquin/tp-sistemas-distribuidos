package common

type Movie struct {
	ID                  string   `json:"id"`
	Title               string   `json:"title"`
	Year                int      `json:"year"`
	Genre               string   `json:"genre"`
	ProductionCountries []string `json:"production_countries"`
}

type Header struct {
	Weight      uint32 `json:"weight"`
	TotalWeight int32  `json:"total_weight"` //-1 if its uknown for the moment
}

type Batch struct {
	Header Header  `json:"header"`
	Movies []Movie `json:"movies"`
}

func (b *Batch) IsEof() bool {
	return b.Header.TotalWeight > 0
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
		Title:               "El secreto de sus ojos",
		Year:                2009,
		Genre:               "Drama",
		ProductionCountries: []string{"Argentina", "España"},
	},
	{
		ID:                  "5",
		Title:               "El padrino",
		Year:                1980,
		Genre:               "Drama",
		ProductionCountries: []string{"España"},
	},
}

var MockedBatch = Batch{
	Header: Header{Weight: uint32(len(mockedMovies)), TotalWeight: int32(-1)},
	Movies: mockedMovies,
}

var EOF = Batch{
	Header: Header{
		TotalWeight: int32(len(MockedBatch.Movies)),
	},
}
