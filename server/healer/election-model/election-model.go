package election_model

type EventType string

const (
	Ok        EventType = "ok"
	Election  EventType = "election"
	Victory   EventType = "victory"
	HeartBeat EventType = "heartbeat"
)

type Event struct {
	Type      EventType `json:"type"`
	Parameter int       `json:"parameter"`
}

type Config struct {
	Id           int
	AmtOfHealers int
}
