package peer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rson9/kamaCache/internal/peer/discovery"
)

type staticDiscoverer struct {
	updates []discovery.Update
}

func (d *staticDiscoverer) Watch(context.Context) (<-chan discovery.Update, error) {
	updates := make(chan discovery.Update, len(d.updates))
	for _, update := range d.updates {
		updates <- update
	}
	close(updates)
	return updates, nil
}

func (d *staticDiscoverer) Close() error { return nil }

func selectedAddr(picker *ConsistentHashPicker, key string) string {
	peer, isSelf := picker.PickPeer(key)
	if isSelf {
		return picker.Addr()
	}
	return peer.Addr()
}

func TestConsistentHashPickerUsesSameOwnerOnEveryNode(t *testing.T) {
	const (
		nodeA = "127.0.0.1:18001"
		nodeB = "127.0.0.1:18002"
		nodeC = "127.0.0.1:18003"
	)

	newPicker := func(self string, peers ...string) *ConsistentHashPicker {
		updates := make([]discovery.Update, 0, len(peers))
		for _, addr := range peers {
			updates = append(updates, discovery.Update{Op: discovery.Add, Addr: addr})
		}
		picker, err := NewConsistentHashPicker(self, &staticDiscoverer{updates: updates})
		if err != nil {
			t.Fatalf("create picker: %v", err)
		}
		return picker
	}

	pickerA := newPicker(nodeA, nodeB, nodeC)
	pickerB := newPicker(nodeB, nodeA, nodeC)
	pickerC := newPicker(nodeC, nodeA, nodeB)
	defer pickerA.Close()
	defer pickerB.Close()
	defer pickerC.Close()

	ready := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		ownerA := selectedAddr(pickerA, "owner-check")
		ownerB := selectedAddr(pickerB, "owner-check")
		ownerC := selectedAddr(pickerC, "owner-check")
		if ownerA == ownerB && ownerA == ownerC {
			ready = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !ready {
		t.Fatal("pickers did not converge on the same owner")
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("owner-check-%d", i)
		ownerA := selectedAddr(pickerA, key)
		ownerB := selectedAddr(pickerB, key)
		ownerC := selectedAddr(pickerC, key)
		if ownerA != ownerB || ownerA != ownerC {
			t.Fatalf("key %q selected different owners: %s, %s, %s", key, ownerA, ownerB, ownerC)
		}
	}
}
