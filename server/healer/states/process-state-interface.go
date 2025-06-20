package states

import (
	concurrencyutils "tp-sistemas-distribuidos/server/healer/concurrency-utils"
	"tp-sistemas-distribuidos/server/healer/election-model"
)

type ProcessState interface {
	HandleMailBox(mailbox chan election_model.Event, peers *concurrencyutils.Peers) (newState ProcessState)
	GetName() (s string)
}
