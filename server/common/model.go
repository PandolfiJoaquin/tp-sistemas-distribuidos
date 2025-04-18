package common

import pkg "pkg/models"

type Movie struct {
	ID                  string        `json:"id"`
	Title               string        `json:"title"`
	Year                int           `json:"year"`
	Genre               string        `json:"genre"`
	ProductionCountries []pkg.Country `json:"production_countries"`
	Budget              uint32        `json:"budget"`
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

type CoutriesBudgetMsg = map[pkg.Country]uint32

type RatingAvg struct {
	RatingSum   uint32 `json:"rating_sum"`
	RatingCount uint32 `json:"rating_count"`
}

type MoviesAvgRatingMsg = map[string]RatingAvg

var mockedMovies = []Movie{
	{
		ID:                  "1",
		Title:               "Interstellar",
		Year:                2010,
		Genre:               "Space",
		ProductionCountries: []pkg.Country{{Code: "GB", Name: "England"}, {Code: "US", Name: "USA"}},
	},
	{
		ID:                  "2",
		Title:               "The Dark Knight",
		Year:                2008,
		Genre:               "Action",
		ProductionCountries: []pkg.Country{{Code: "US", Name: "USA"}},
	},
	{
		ID:                  "3",
		Title:               "Rata blanca",
		Year:                2011,
		Genre:               "Comedy",
		ProductionCountries: []pkg.Country{{Code: "AR", Name: "Argentina"}},
	},
	{
		ID:                  "4",
		Title:               "El secreto de sus ojos",
		Year:                2009,
		Genre:               "Drama",
		ProductionCountries: []pkg.Country{{Code: "AR", Name: "Argentina"}, {Code: "ES", Name: "Spain"}},
	},
	{
		ID:                  "5",
		Title:               "El padrino",
		Year:                1980,
		Genre:               "Drama",
		ProductionCountries: []pkg.Country{{Code: "ES", Name: "Spain"}},
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
