package raft

import (
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	pb "github.com/coreos/etcd/raft/raftpb"
)

// MultiNode represents a node that is participating in multiple consensus groups.
type MultiNode interface {
	CreateGroup(group uint64, peers []Peer, storage Storage) error
	Tick()
	Propose(ctx context.Context, group uint64, data []byte) error
	ProposeConfChange(ctx context.Context, group uint64, cc pb.ConfChange) error
	ApplyConfChange(group uint64, cc pb.ConfChange)
	Step(ctx context.Context, group uint64, msg pb.Message) error
	Ready() <-chan map[uint64]Ready
	Advance()
	Stop()
	// TODO: Add Compact. Is Campaign necessary?
}

// StartMultiNode creates a MultiNode and starts its background goroutine.
func StartMultiNode(id uint64, election, heartbeat int) MultiNode {
	mn := newMultiNode(id, election, heartbeat)
	go mn.run()
	return &mn
}

// TODO(bdarnell): add group ID to the underlying protos?
type multiMessage struct {
	group uint64
	msg   pb.Message
}

type multiConfChange struct {
	group uint64
	msg   pb.ConfChange
}

type groupCreation struct {
	id      uint64
	peers   []Peer
	storage Storage
	// TODO(bdarnell): do we really need the done channel here? It's
	// unlike the rest of this package, but we need the group creation
	// to be complete before any Propose or other calls.
	done chan struct{}
}

type multiNode struct {
	id        uint64
	election  int
	heartbeat int
	groupc    chan groupCreation
	propc     chan multiMessage
	recvc     chan multiMessage
	confc     chan multiConfChange
	readyc    chan map[uint64]Ready
	advancec  chan struct{}
	tickc     chan struct{}
	done      chan struct{}
}

func newMultiNode(id uint64, election, heartbeat int) multiNode {
	return multiNode{
		id:        id,
		election:  election,
		heartbeat: heartbeat,
		groupc:    make(chan groupCreation),
		propc:     make(chan multiMessage),
		recvc:     make(chan multiMessage),
		confc:     make(chan multiConfChange),
		readyc:    make(chan map[uint64]Ready),
		advancec:  make(chan struct{}),
		tickc:     make(chan struct{}),
		done:      make(chan struct{}),
	}
}

type groupState struct {
	id         uint64
	raft       *raft
	prevSoftSt *SoftState
	prevHardSt pb.HardState
	prevSnapi  uint64
}

func (g *groupState) newReady() Ready {
	return newReady(g.raft, g.prevSoftSt, g.prevHardSt)
}

func (g *groupState) commitReady(rd Ready) {
	if rd.SoftState != nil {
		g.prevSoftSt = rd.SoftState
	}
	if !IsEmptyHardState(rd.HardState) {
		g.prevHardSt = rd.HardState
	}
	if !IsEmptySnap(rd.Snapshot) {
		g.prevSnapi = rd.Snapshot.Metadata.Index
	}
	if len(rd.Entries) > 0 {
		// TODO(bdarnell): stableTo(rd.Snapshot.Index) if any
		g.raft.raftLog.stableTo(rd.Entries[len(rd.Entries)-1].Index)
	}
	g.raft.raftLog.appliedTo(rd.HardState.Commit)

	g.raft.msgs = nil
}

func (mn *multiNode) run() {
	groups := map[uint64]*groupState{}
	rds := map[uint64]Ready{}
	var advancec chan struct{}
	for {
		readyc := mn.readyc
		if len(rds) == 0 || advancec != nil {
			readyc = nil
		}

		var group *groupState
		select {
		case gc := <-mn.groupc:
			r := newRaft(mn.id, nil, mn.election, mn.heartbeat, gc.storage)
			snap, err := r.raftLog.snapshot()
			if err != nil {
				panic(err) // TODO(bdarnell)
			}
			group = &groupState{
				id:         gc.id,
				raft:       r,
				prevSoftSt: r.softState(),
				prevHardSt: r.HardState,
				prevSnapi:  snap.Metadata.Index,
			}
			groups[gc.id] = group
			ents := make([]pb.Entry, len(gc.peers))
			for i, peer := range gc.peers {
				cc := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: peer.ID, Context: peer.Context}
				data, err := cc.Marshal()
				if err != nil {
					panic("unexpected marshal error")
				}
				ents[i] = pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: uint64(i + 1), Data: data}
			}
			r.raftLog.append(0, ents...)
			r.raftLog.committed = uint64(len(ents))
			close(gc.done)
		case mm := <-mn.propc:
			// TODO(bdarnell): single-node impl doesn't read from propc unless the group
			// has a leader; we can't do that since we have one propc for many groups.
			// We'll have to buffer somewhere on a group-by-group basis.
			mm.msg.From = mn.id
			group = groups[mm.group]
			group.raft.Step(mm.msg)
		case mm := <-mn.recvc:
			group = groups[mm.group]
			group.raft.Step(mm.msg)
		case mcc := <-mn.confc:
			group = groups[mcc.group]
			switch mcc.msg.Type {
			case pb.ConfChangeAddNode:
				group.raft.addNode(mcc.msg.NodeID)
			case pb.ConfChangeRemoveNode:
				group.raft.removeNode(mcc.msg.NodeID)
			default:
				panic("unexpected conf type")
			}
		case <-mn.tickc:
			// TODO(bdarnell): instead of calling every group on every tick,
			// we should have a priority queue of groups based on their next
			// time-based event.
			for _, g := range groups {
				g.raft.tick()
				rd := g.newReady()
				if rd.containsUpdates() {
					rds[g.id] = rd
				}
			}
		case readyc <- rds:
			advancec = mn.advancec
		case <-advancec:
			for group, rd := range rds {
				groups[group].commitReady(rd)
			}
			rds = map[uint64]Ready{}
			advancec = nil
		case <-mn.done:
			return
		}
		if group != nil {
			rd := group.newReady()
			if rd.containsUpdates() {
				rds[group.id] = rd
			}
		}
	}
}

func (mn *multiNode) CreateGroup(id uint64, peers []Peer, storage Storage) error {
	gc := groupCreation{
		id:      id,
		peers:   peers,
		storage: storage,
		done:    make(chan struct{}),
	}
	mn.groupc <- gc
	select {
	case <-gc.done:
		return nil
	case <-mn.done:
		return ErrStopped
	}
}

func (mn *multiNode) Stop() {
	close(mn.done)
}

func (mn *multiNode) Tick() {
	select {
	case mn.tickc <- struct{}{}:
	case <-mn.done:
	}
}

func (mn *multiNode) Propose(ctx context.Context, group uint64, data []byte) error {
	return mn.step(ctx, multiMessage{group,
		pb.Message{
			Type: pb.MsgProp,
			Entries: []pb.Entry{
				{Data: data},
			},
		}})
}

func (mn *multiNode) ProposeConfChange(ctx context.Context, group uint64, cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	return mn.Step(ctx, group,
		pb.Message{
			Type: pb.MsgProp,
			Entries: []pb.Entry{
				{Type: pb.EntryConfChange, Data: data},
			},
		})
}

func (mn *multiNode) step(ctx context.Context, m multiMessage) error {
	ch := mn.recvc
	if m.msg.Type == pb.MsgProp {
		ch = mn.propc
	}

	select {
	case ch <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-mn.done:
		return ErrStopped
	}
}

func (mn *multiNode) ApplyConfChange(group uint64, cc pb.ConfChange) {
	select {
	case mn.confc <- multiConfChange{group, cc}:
	case <-mn.done:
	}
}

func (mn *multiNode) Step(ctx context.Context, group uint64, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if m.Type == pb.MsgHup || m.Type == pb.MsgBeat {
		// TODO: return an error?
		return nil
	}
	return mn.step(ctx, multiMessage{group, m})
}

func (mn *multiNode) Ready() <-chan map[uint64]Ready {
	return mn.readyc
}

func (mn *multiNode) Advance() {
	select {
	case mn.advancec <- struct{}{}:
	case <-mn.done:
	}
}
