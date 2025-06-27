package concurrency_utils

import (
	"sync"
	electionmodel "tp-sistemas-distribuidos/server/healer/election-model"
)

type Peers struct {
	peers map[int]chan electionmodel.Event
	mutex sync.RWMutex
}

func NewPeers() *Peers {
	return &Peers{
		peers: make(map[int]chan electionmodel.Event),
	}
}

func (sm *Peers) SafeAddPeer(id int, mailbox chan electionmodel.Event) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.peers[id] = mailbox
}

func (sm *Peers) SafeRemovePeer(id int) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	delete(sm.peers, id)
}

func (sm *Peers) SafeContains(id int) bool {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	_, ok := sm.peers[id]
	return ok
}

func (sm *Peers) Broadcast(event electionmodel.Event) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	for _, peer := range sm.peers {
		peer <- event
	}
}

func (sm *Peers) SendToId(event electionmodel.Event, id int) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	sm.peers[id] <- event
}
