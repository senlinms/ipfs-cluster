package raft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	hashiraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	libp2praft "github.com/libp2p/go-libp2p-raft"
)

// RaftMaxSnapshots indicates how many snapshots to keep in the consensus data
// folder.
var RaftMaxSnapshots = 5

// is this running 64 bits arch? https://groups.google.com/forum/#!topic/golang-nuts/vAckmhUMAdQ
const sixtyfour = uint64(^uint(0)) == ^uint64(0)

// Raft performs all Raft-specific operations which are needed by Cluster but
// are not fulfilled by the consensus interface. It should contain most of the
// Raft-related stuff so it can be easily replaced in the future, if need be.
type Raft struct {
	raft          *hashiraft.Raft
	transport     *libp2praft.Libp2pTransport
	snapshotStore hashiraft.SnapshotStore
	logStore      hashiraft.LogStore
	stableStore   hashiraft.StableStore
	peerstore     *libp2praft.Peerstore
	boltdb        *raftboltdb.BoltStore
	dataFolder    string
}

// NewRaft launches a go-libp2p-raft consensus peer.
func NewRaft(peers []peer.ID, host host.Host, cfg *Config, fsm hashiraft.FSM) (*Raft, error) {
	logger.Debug("creating libp2p Raft transport")
	transport, err := libp2praft.NewLibp2pTransportWithHost(host)
	if err != nil {
		logger.Error("creating libp2p-raft transport: ", err)
		return nil, err
	}

	pstore := &libp2praft.Peerstore{}
	peersStr := make([]string, len(peers), len(peers))
	for i, p := range peers {
		peersStr[i] = peer.IDB58Encode(p)
	}
	pstore.SetPeers(peersStr)

	logger.Debug("creating file snapshot store")
	dataFolder := cfg.DataFolder
	if dataFolder == "" {
		dataFolder = filepath.Join(cfg.BaseDir, DefaultDataSubFolder)
	}

	err = os.MkdirAll(dataFolder, 0700)
	if err != nil {
		logger.Errorf("creating cosensus data folder (%s): %s",
			dataFolder, err)
		return nil, err
	}
	snapshots, err := hashiraft.NewFileSnapshotStoreWithLogger(dataFolder, RaftMaxSnapshots, raftStdLogger)
	if err != nil {
		logger.Error("creating file snapshot store: ", err)
		return nil, err
	}

	logger.Debug("creating BoltDB log store")
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataFolder, "raft.db"))
	if err != nil {
		logger.Error("creating bolt store: ", err)
		return nil, err
	}

	logger.Debug("creating Raft")
	r, err := hashiraft.NewRaft(cfg.HashiraftCfg, fsm, logStore, logStore, snapshots, pstore, transport)
	if err != nil {
		logger.Error("initializing raft: ", err)
		return nil, err
	}

	raft := &Raft{
		raft:          r,
		transport:     transport,
		snapshotStore: snapshots,
		logStore:      logStore,
		stableStore:   logStore,
		peerstore:     pstore,
		boltdb:        logStore,
		dataFolder:    dataFolder,
	}

	return raft, nil
}

// WaitForLeader holds until Raft says we have a leader.
// Returns an error if we don't.
func (r *Raft) WaitForLeader(ctx context.Context) error {
	// Using Raft observers panics on non-64 architectures.
	// This is a work around
	if sixtyfour {
		return r.waitForLeader(ctx)
	}
	return r.waitForLeaderLegacy(ctx)
}

func (r *Raft) waitForLeader(ctx context.Context) error {
	obsCh := make(chan hashiraft.Observation, 1)
	filter := func(o *hashiraft.Observation) bool {
		switch o.Data.(type) {
		case hashiraft.LeaderObservation:
			return true
		default:
			return false
		}
	}
	observer := hashiraft.NewObserver(obsCh, false, filter)
	r.raft.RegisterObserver(observer)
	defer r.raft.DeregisterObserver(observer)
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case obs := <-obsCh:
			switch obs.Data.(type) {
			case hashiraft.LeaderObservation:
				leaderObs := obs.Data.(hashiraft.LeaderObservation)
				logger.Infof("Raft Leader elected: %s", leaderObs.Leader)
				return nil
			}
		case <-ticker.C:
			if l := r.raft.Leader(); l != "" { //we missed or there was no election
				logger.Debug("waitForleaderTimer")
				logger.Infof("Raft Leader elected: %s", l)
				ticker.Stop()
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// 32-bit systems should use this.
func (r *Raft) waitForLeaderLegacy(ctx context.Context) error {
	for {
		leader := r.raft.Leader()
		if leader != "" {
			logger.Infof("Raft Leader elected: %s", leader)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// WaitForUpdates holds until Raft has synced to the last index in the log
func (r *Raft) WaitForUpdates(ctx context.Context) error {
	logger.Debug("Raft state is catching up")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			lai := r.raft.AppliedIndex()
			li := r.raft.LastIndex()
			logger.Debugf("current Raft index: %d/%d",
				lai, li)
			if lai == li {
				return nil
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Snapshot tells Raft to take a snapshot.
func (r *Raft) Snapshot() error {
	future := r.raft.Snapshot()
	err := future.Error()
	if err != nil && !strings.Contains(err.Error(), "Nothing new to snapshot") {
		return errors.New("could not take snapshot: " + err.Error())
	}
	return nil
}

// Shutdown shutdown Raft and closes the BoltDB.
func (r *Raft) Shutdown() error {
	future := r.raft.Shutdown()
	err := future.Error()
	errMsgs := ""
	if err != nil {
		errMsgs += "could not shutdown raft: " + err.Error() + ".\n"
	}

	err = r.boltdb.Close() // important!
	if err != nil {
		errMsgs += "could not close boltdb: " + err.Error()
	}

	if errMsgs != "" {
		return errors.New(errMsgs)
	}

	// If the shutdown worked correctly
	// (including snapshot) we can remove the Raft
	// database (which traces peers additions
	// and removals). It makes re-start of the peer
	// way less confusing for Raft while the state
	// can be restored from the snapshot.
	//os.Remove(filepath.Join(r.dataFolder, "raft.db"))
	return nil
}

// AddPeer adds a peer to Raft
func (r *Raft) AddPeer(peer string) error {
	if r.hasPeer(peer) {
		logger.Debug("skipping raft add as already in peer set")
		return nil
	}

	future := r.raft.AddPeer(peer)
	err := future.Error()
	if err != nil {
		logger.Error("raft cannot add peer: ", err)
		return err
	}
	peers, _ := r.peerstore.Peers()
	logger.Debugf("raft peerstore: %s", peers)
	return err
}

// RemovePeer removes a peer from Raft
func (r *Raft) RemovePeer(peer string) error {
	if !r.hasPeer(peer) {
		return nil
	}

	future := r.raft.RemovePeer(peer)
	err := future.Error()
	if err != nil {
		logger.Error("raft cannot remove peer: ", err)
		return err
	}
	peers, _ := r.peerstore.Peers()
	logger.Debugf("raft peerstore: %s", peers)
	return err
}

// func (r *Raft) SetPeers(peers []string) error {
//	logger.Debugf("SetPeers(): %s", peers)
//	future := r.raft.SetPeers(peers)
//	err := future.Error()
//	if err != nil {
//		logger.Error(err)
//	}
//	return err
// }

// Leader returns Raft's leader. It may be an empty string if
// there is no leader or it is unknown.
func (r *Raft) Leader() string {
	return r.raft.Leader()
}

func (r *Raft) hasPeer(peer string) bool {
	found := false
	peers, _ := r.peerstore.Peers()
	for _, p := range peers {
		if p == peer {
			found = true
			break
		}
	}

	return found
}
