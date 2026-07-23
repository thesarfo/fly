package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n *maelstrom.Node

	mu        sync.Mutex
	seen      map[int]struct{}
	neighbors []string
	pending   map[string]map[int]struct{}
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		n:       n,
		seen:    make(map[int]struct{}),
		pending: make(map[string]map[int]struct{}),
	}
}

func (s *server) addIfNew(m int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[m]; ok {
		return false
	}
	s.seen[m] = struct{}{}
	return true
}

func (s *server) queue(messages []int, exclude string) {
	if len(messages) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, neighbor := range s.neighbors {
		if neighbor == exclude {
			continue
		}
		set, ok := s.pending[neighbor]
		if !ok {
			set = make(map[int]struct{})
			s.pending[neighbor] = set
		}
		for _, m := range messages {
			set[m] = struct{}{}
		}
	}
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if s.addIfNew(body.Message) {
		s.queue([]int{body.Message}, msg.Src)
	}

	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body struct {
		Messages []int `json:"messages"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var fresh []int
	for _, m := range body.Messages {
		if s.addIfNew(m) {
			fresh = append(fresh, m)
		}
	}
	s.queue(fresh, msg.Src)

	return s.n.Reply(msg, map[string]any{"type": "gossip_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	messages := make([]int, 0, len(s.seen))
	for v := range s.seen {
		messages = append(messages, v)
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var body struct {
		Topology map[string][]string `json:"topology"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	s.neighbors = body.Topology[s.n.ID()]
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) gossipLoop() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		batches := make(map[string][]int, len(s.pending))
		for neighbor, set := range s.pending {
			if len(set) == 0 {
				continue
			}
			msgs := make([]int, 0, len(set))
			for m := range set {
				msgs = append(msgs, m)
			}
			batches[neighbor] = msgs
		}
		s.mu.Unlock()

		for neighbor, msgs := range batches {
			neighbor, msgs := neighbor, msgs
			s.n.RPC(neighbor, map[string]any{
				"type":     "gossip",
				"messages": msgs,
			}, func(maelstrom.Message) error {
				s.mu.Lock()
				if set, ok := s.pending[neighbor]; ok {
					for _, m := range msgs {
						delete(set, m)
					}
				}
				s.mu.Unlock()
				return nil
			})
		}
	}
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("gossip", s.handleGossip)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)

	go s.gossipLoop()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
