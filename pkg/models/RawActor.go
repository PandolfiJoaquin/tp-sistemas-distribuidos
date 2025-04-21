package models

type CastMember struct {
	CastID      int     `json:"cast_id"`
	Character   string  `json:"character"`
	CreditID    string  `json:"credit_id"`
	Gender      int     `json:"gender"`
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	Order       int     `json:"order"`
	ProfilePath *string `json:"profile_path"` // Nullable
}

type CrewMember struct {
	CreditID    string  `json:"credit_id"`
	Department  string  `json:"department"`
	Gender      int     `json:"gender"`
	ID          int     `json:"id"`
	Job         string  `json:"job"`
	Name        string  `json:"name"`
	ProfilePath *string `json:"profile_path"` // Nullable
}

type RawCredits struct {
	Cast    []CastMember `json:"cast"`
	Crew    []CrewMember `json:"crew"`
	MovieId string       `json:"movie_id"`
}
