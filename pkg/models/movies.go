package models

type Movie struct {
	Adult               bool          `json:"adult"`
	BelongsToCollection *Collection   `json:"belongs_to_collection,omitempty"`
	Budget              uint64         `json:"budget"`
	Genres              []Genre       `json:"genres"`
	Homepage            string        `json:"homepage,omitempty"`
	ID                  int64         `json:"id"`
	IMDBID              string        `json:"imdb_id,omitempty"`
	OriginalLanguage    string        `json:"original_language"`
	OriginalTitle       string        `json:"original_title"`
	Overview            string        `json:"overview,omitempty"`
	Popularity          float64       `json:"popularity"`
	PosterPath          string        `json:"poster_path,omitempty"`
	ProductionCompanies []Company     `json:"production_companies"`
	ProductionCountries []Country     `json:"production_countries"`
	ReleaseDate         string        `json:"release_date"` 
	Revenue             int64         `json:"revenue,omitempty"`
	Runtime             float64       `json:"runtime,omitempty"`
	SpokenLanguages     []Language    `json:"spoken_languages"`
	Status              string        `json:"status,omitempty"`
	Tagline             string        `json:"tagline,omitempty"`
	Title               string        `json:"title"`
	Video               bool          `json:"video"`
	VoteAverage         float64       `json:"vote_average"`
	VoteCount           int64         `json:"vote_count"`
}

// Collection represents the belongs_to_collection field
type Collection struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	PosterPath   string `json:"poster_path,omitempty"`
	BackdropPath string `json:"backdrop_path,omitempty"`
}

// Genre represents individual genre in genres array
type Genre struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// Company represents production company
type Company struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// Country represents production country
type Country struct {
	ISO3166_1 string `json:"iso_3166_1"`
	Name      string `json:"name"`
}

// Language represents spoken language
type Language struct {
	ISO639_1 string `json:"iso_639_1"`
	Name     string `json:"name"`
}


