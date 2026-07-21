package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	seen := make(map[float64]struct{})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		seen[body["message"].(float64)] = struct{}{}
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		messages := make([]float64, 0, len(seen))
		for v := range seen {
			messages = append(messages, v)
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
