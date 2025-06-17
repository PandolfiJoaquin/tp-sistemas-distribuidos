package states

import "tp-sistemas-distribuidos/server/healer/election-model"

type ProcessState interface {
	HandleMailBox(mailbox chan election_model.Event) (newState ProcessState)
}
