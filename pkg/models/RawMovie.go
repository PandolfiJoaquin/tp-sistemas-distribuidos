package models

import (
	"encoding/json"
	"time"
)

type RawMovie struct {
	Adult               bool        `json:"adult"`
	BelongsToCollection *Collection `json:"belongs_to_collection,omitempty"`
	Budget              uint64      `json:"budget"`
	Genres              []Genre     `json:"genres"`
	Homepage            string      `json:"homepage,omitempty"`
	ID                  uint32      `json:"id"`
	IMDBID              string      `json:"imdb_id,omitempty"`
	OriginalLanguage    string      `json:"original_language"`
	OriginalTitle       string      `json:"original_title"`
	Overview            string      `json:"overview,omitempty"`
	Popularity          float32     `json:"popularity"`
	PosterPath          string      `json:"poster_path,omitempty"`
	ProductionCompanies []Company   `json:"production_companies"`
	ProductionCountries []Country   `json:"production_countries"`
	ReleaseDate         time.Time   `json:"release_date"`
	Revenue             uint64      `json:"revenue,omitempty"`
	Runtime             float32     `json:"runtime,omitempty"`
	SpokenLanguages     []Language  `json:"spoken_languages"`
	Status              string      `json:"status,omitempty"`
	Tagline             string      `json:"tagline,omitempty"`
	Title               string      `json:"title"`
	Video               bool        `json:"video"`
	VoteAverage         float32     `json:"vote_average"`
	VoteCount           uint32      `json:"vote_count"`
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
	Code string `json:"iso_3166_1"` // ISO 3166-1 alpha-2 code
	Name string `json:"name"`
}

// Language represents spoken language
type Language struct {
	Code string `json:"iso_639_1"` // ISO 639-1 code
	Name string `json:"name"`
}

func (c Country) Equals(other Country) bool {
	return c.Code == other.Code && c.Name == other.Name
}

// MarshalJSON implementa json.Marshaler
func (c Country) MarshalJSON() ([]byte, error) {
	// Definimos una struct auxiliar con los campos que queremos serializar
	type Alias Country
	return json.Marshal(&struct {
		Alias
	}{
		Alias: (Alias)(c),
	})
}

// UnmarshalJSON implementa json.Unmarshaler
func (c *Country) UnmarshalJSON(data []byte) error {
	type Alias Country
	aux := &struct {
		Alias
	}{}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	*c = Country(aux.Alias)
	return nil
}
