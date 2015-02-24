package raft

import (
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	pb "github.com/coreos/etcd/raft/raftpb"
)

// MultiNode represents a node that is participating in multiple consensus groups.
type MultiNode interface {
	CreateGroup(group uint64, peers []Peer, storage Storage) error
	RemoveGroup(group uint64) error
	Tick()
	Campaign(ctx context.Context, group uint64) error
	Propose(ctx context.Context, group uint64, data []byte) error
	ProposeConfChange(ctx context.Context, group uint64, cc pb.ConfChange) error
	ApplyConfChange(group uint64, cc pb.ConfChange) *pb.ConfState
	Step(ctx context.Context, group uint64, msg pb.Message) error
	Ready() <-chan map[uint64]Ready
	// Advance() must be called with the last value returned from the Ready() channel.
	Advance(map[uint64]Ready)
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
	ch    chan pb.ConfState
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

type groupRemoval struct {
	id uint64
	// TODO(bdarnell): see comment on groupCreation.done
	done chan struct{}
}

type multiNode struct {
	id        uint64
	election  int
	heartbeat int
	groupc    chan groupCreation
	rmgroupc  chan groupRemoval
	propc     chan multiMessage
	recvc     chan multiMessage
	confc     chan multiConfChange
	readyc    chan map[uint64]Ready
	advancec  chan map[uint64]Ready
	tickc     chan struct{}
	done      chan struct{}
}

func newMultiNode(id uint64, election, heartbeat int) multiNode {
	return multiNode{
		id:        id,
		election:  election,
		heartbeat: heartbeat,
		groupc:    make(chan groupCreation),
		rmgroupc:  make(chan groupRemoval),
		propc:     make(chan multiMessage),
		recvc:     make(chan multiMessage),
		confc:     make(chan multiConfChange),
		readyc:    make(chan map[uint64]Ready),
		advancec:  make(chan map[uint64]Ready),
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
		g.raft.raftLog.stableSnapTo(g.prevSnapi)
	}
	if len(rd.Entries) > 0 {
		// TODO(bdarnell): stableTo(rd.Snapshot.Index) if any
		e := rd.Entries[len(rd.Entries)-1]
		g.raft.raftLog.stableTo(e.Index, e.Term)
	}

	// TODO(bdarnell): in node.go, Advance() ignores CommittedEntries and calls
	// appliedTo with HardState.Commit, but this causes problems in multinode/cockroach.
	// The two should be the same except for the special-casing of the initial ConfChange
	// entries.
	if len(rd.CommittedEntries) > 0 {
		g.raft.raftLog.appliedTo(rd.CommittedEntries[len(rd.CommittedEntries)-1].Index)
	}
	//g.raft.raftLog.appliedTo(rd.HardState.Commit)
}

func (mn *multiNode) run() {
	groups := map[uint64]*groupState{}
	rds := map[uint64]Ready{}
	var advancec chan map[uint64]Ready
	for {
		readyc := mn.readyc
		if len(rds) == 0 || advancec != nil {
			readyc = nil
		}

		var group *groupState
		select {
		case gc := <-mn.groupc:
			// TODO(bdarnell): pass applied through gc and into newRaft.
			r := newRaft(mn.id, nil, mn.election, mn.heartbeat, gc.storage, 0)
			group = &groupState{
				id:         gc.id,
				raft:       r,
				prevSoftSt: r.softState(),
				prevHardSt: r.HardState,
			}
			groups[gc.id] = group
			lastIndex, err := gc.storage.LastIndex()
			if err != nil {
				panic(err) // TODO(bdarnell)
			}
			// If the log is empty, this is a new group (like StartNode); otherwise it's
			// restoring an existing group (like RestartNode).
			// TODO(bdarnell): rethink group initialization and whether the application needs
			// to be able to tell us when it expects the group to exist.
			if lastIndex == 0 {
				ents := make([]pb.Entry, len(gc.peers))
				for i, peer := range gc.peers {
					cc := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: peer.ID, Context: peer.Context}
					data, err := cc.Marshal()
					if err != nil {
						panic("unexpected marshal error")
					}
					ents[i] = pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: uint64(i + 1), Data: data}
				}
				r.raftLog.append(ents...)
				r.raftLog.committed = uint64(len(ents))
				for _, peer := range gc.peers {
					r.addNode(peer.ID)
				}
			}
			close(gc.done)

		case gr := <-mn.rmgroupc:
			delete(groups, gr.id)
			delete(rds, gr.id)
			close(gr.done)

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
			if mcc.msg.NodeID == None {
				group.raft.resetPendingConf()
				select {
				case mcc.ch <- pb.ConfState{Nodes: group.raft.nodes()}:
				case <-mn.done:
				}
				break
			}
			switch mcc.msg.Type {
			case pb.ConfChangeAddNode:
				group.raft.addNode(mcc.msg.NodeID)
			case pb.ConfChangeRemoveNode:
				group.raft.removeNode(mcc.msg.NodeID)
			case pb.ConfChangeUpdateNode:
				group.raft.resetPendingConf()
			default:
				panic("unexpected conf type")
			}
			select {
			case mcc.ch <- pb.ConfState{Nodes: group.raft.nodes()}:
			case <-mn.done:
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
			// Clear outgoing messages as soon as we've passed them to the application.
			for g := range rds {
				groups[g].raft.msgs = nil
			}
			rds = map[uint64]Ready{}
			advancec = mn.advancec

		case advs := <-advancec:
			for groupID, rd := range advs {
				group, ok := groups[groupID]
				if !ok {
					continue
				}
				group.commitReady(rd)

				// We've been accumulating new entries in rds which may now be obsolete.
				// Drop the old Ready object and create a new one if needed.
				delete(rds, groupID)
				newRd := group.newReady()
				if newRd.containsUpdates() {
					rds[groupID] = newRd
				}
			}
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

func (mn *multiNode) RemoveGroup(id uint64) error {
	gr := groupRemoval{
		id:   id,
		done: make(chan struct{}),
	}
	mn.rmgroupc <- gr
	select {
	case <-gr.done:
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

func (mn *multiNode) Campaign(ctx context.Context, group uint64) error {
	return mn.step(ctx, multiMessage{group,
		pb.Message{
			Type: pb.MsgHup,
		},
	})
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

func (mn *multiNode) ApplyConfChange(group uint64, cc pb.ConfChange) *pb.ConfState {
	mcc := multiConfChange{group, cc, make(chan pb.ConfState)}
	select {
	case mn.confc <- mcc:
	case <-mn.done:
	}
	select {
	case cs := <-mcc.ch:
		return &cs
	case <-mn.done:
		// Per comments on Node.ApplyConfChange, this method should never return nil.
		return &pb.ConfState{}
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

func (mn *multiNode) Advance(rds map[uint64]Ready) {
	select {
	case mn.advancec <- rds:
	case <-mn.done:
	}
}
