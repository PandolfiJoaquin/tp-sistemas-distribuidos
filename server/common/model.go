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

type CountryBudget struct {
	Country pkg.Country `json:"country"`
	Budget  uint32      `json:"budget"`
}

type CoutriesBudgetMsg struct {
	Header    Header          `json:"header"`
	Countries []CountryBudget `json:"countries"`
}

func (b *CoutriesBudgetMsg) IsEof() bool { //TODO: se puede obviar si se compone con el Header y le pongo el metodo al header
	return b.Header.TotalWeight > 0
}

type MovieAvgRating struct {
	MovieID     string `json:"movie_id"`
	RatingSum   uint32 `json:"rating_sum"`
	RatingCount uint32 `json:"rating_count"`
}

type MoviesAvgRatingMsg struct {
	Header        Header           `json:"header"`
	MoviesRatings []MovieAvgRating `json:"movies_ratings"`
}

func (b *MoviesAvgRatingMsg) IsEof() bool { //TODO: se puede obviar si se compone con el Header y le pongo el metodo al header
	return b.Header.TotalWeight > 0
}

type BestAndWorstMovies struct {
	BestMovie  string `json:"best_movie"`
	WorstMovie string `json:"worst_movie"`
}

type Top5Countries struct {
	FirstCountry  pkg.Country `json:"first_country"`
	SecondCountry pkg.Country `json:"second_country"`
	ThirdCountry  pkg.Country `json:"third_country"`
	FourthCountry pkg.Country `json:"fourth_country"`
	FifthCountry  pkg.Country `json:"fifth_country"`
}

var mockedMovies = []Movie{
	{
		ID:                  "1",
		Title:               "Interstellar",
		Year:                2010,
		Genre:               "Space",
		ProductionCountries: []pkg.Country{{Code: "GB", Name: "England"}, {Code: "US", Name: "USA"}},
		Budget:              100000000,
	},
	{
		ID:                  "2",
		Title:               "The Dark Knight",
		Year:                2008,
		Genre:               "Action",
		ProductionCountries: []pkg.Country{{Code: "US", Name: "USA"}},
		Budget:              280000000,
	},
	{
		ID:                  "3",
		Title:               "Rata blanca",
		Year:                2011,
		Genre:               "Comedy",
		ProductionCountries: []pkg.Country{{Code: "AR", Name: "Argentina"}},
		Budget:              930000000,
	},
	{
		ID:                  "4",
		Title:               "El secreto de sus ojos",
		Year:                2009,
		Genre:               "Drama",
		ProductionCountries: []pkg.Country{{Code: "AR", Name: "Argentina"}, {Code: "ES", Name: "Spain"}},
		Budget:              69200000,
	},
	{
		ID:                  "5",
		Title:               "El padrino",
		Year:                1980,
		Genre:               "Drama",
		ProductionCountries: []pkg.Country{{Code: "ES", Name: "Spain"}},
		Budget:              34000000,
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
