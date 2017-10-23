package raft

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"time"

	"github.com/ipfs/ipfs-cluster/config"

	hashiraft "github.com/hashicorp/raft"
)

// ConfigKey is the default configuration key for holding this component's
// configuration section.
var configKey = "raft"

// Configuration defaults
var (
	DefaultDataSubFolder        = "ipfs-cluster-data"
	DefaultWaitForLeaderTimeout = 15 * time.Second
	DefaultCommitRetries        = 1
)

// Config allows to configure the Raft Consensus component for ipfs-cluster.
// The component's configuration section is represented by ConfigJSON.
// Config implements the ComponentConfig interface.
type Config struct {
	config.Saver

	// A Hashicorp Raft's configuration object.
	HashiraftCfg *hashiraft.Config
	// A folder to store Raft's data.
	DataFolder string
	// LeaderTimeout specifies how long to wait for a leader before
	// failing an operation.
	WaitForLeaderTimeout time.Duration
	// CommitRetries specifies how many times we retry a failed commit until
	// we give up
	CommitRetries int
}

// ConfigJSON represents a human-friendly Config
// object which can be saved to JSON.  Most configuration keys are converted
// into simple types like strings, and key names aim to be self-explanatory
// for the user.
// Check https://godoc.org/github.com/hashicorp/raft#Config for extended
// description on all Raft-specific keys.
type jsonConfig struct {
	// Storage folder for snapshots, log store etc. Used by
	// the Raft.
	DataFolder string `json:"data_folder,omitempty"`

	// How long to wait for a leader before failing
	WaitForLeaderTimeout string `json:"wait_for_leader_timeout"`

	// How many retries to make upon a failed commit
	CommitRetries int `json:"commit_retries"`

	// HeartbeatTimeout specifies the time in follower state without
	// a leader before we attempt an election.
	HeartbeatTimeout string `json:"heartbeat_timeout,omitempty"`

	// ElectionTimeout specifies the time in candidate state without
	// a leader before we attempt an election.
	ElectionTimeout string `json:"election_timeout,omitempty"`

	// CommitTimeout controls the time without an Apply() operation
	// before we heartbeat to ensure a timely commit.
	CommitTimeout string `json:"commit_timeout,omitempty"`

	// MaxAppendEntries controls the maximum number of append entries
	// to send at once.
	MaxAppendEntries int `json:"max_append_entries,omitempty"`

	// If we are a member of a cluster, and RemovePeer is invoked for the
	// local node, then we forget all peers and transition into the
	// follower state.
	// If ShutdownOnRemove is is set, we additional shutdown Raft.
	// Otherwise, we can become a leader of a cluster containing
	// only this node.
	ShutdownOnRemove bool `json:"shutdown_on_remove,omitempty"`

	// TrailingLogs controls how many logs we leave after a snapshot.
	TrailingLogs uint64 `json:"trailing_logs,omitempty"`

	// SnapshotInterval controls how often we check if we should perform
	// a snapshot.
	SnapshotInterval string `json:"snapshot_interval,omitempty"`

	// SnapshotThreshold controls how many outstanding logs there must be
	// before we perform a snapshot.
	SnapshotThreshold uint64 `json:"snapshot_threshold,omitempty"`

	// LeaderLeaseTimeout is used to control how long the "lease" lasts
	// for being the leader without being able to contact a quorum
	// of nodes. If we reach this interval without contact, we will
	// step down as leader.
	LeaderLeaseTimeout string `json:"leader_lease_timeout,omitempty"`

	// StartAsLeader forces Raft to start in the leader state. This should
	// never be used except for testing purposes, as it can cause a split-brain.
	StartAsLeader bool `json:"start_as_leader,omitempty"`

	// The unique ID for this server across all time. When running with
	// ProtocolVersion < 3, you must set this to be the same as the network
	// address of your transport.
	// LocalID string `json:local_id`
}

// ConfigKey returns a human-friendly indentifier for this Config.
func (cfg *Config) ConfigKey() string {
	return configKey
}

// Validate checks that this configuration has working values,
// at least in appereance.
func (cfg *Config) Validate() error {
	if cfg.HashiraftCfg == nil {
		return errors.New("No hashicorp/raft.Config")
	}
	if cfg.WaitForLeaderTimeout <= 0 {
		return errors.New("wait_for_leader_timeout <= 0")
	}

	if cfg.CommitRetries < 0 {
		return errors.New("commit_retries is invalid")
	}

	return hashiraft.ValidateConfig(cfg.HashiraftCfg)
}

// LoadJSON parses a json-encoded configuration (see jsonConfig).
// The Config will have default values for all fields not explicited
// in the given json object.
func (cfg *Config) LoadJSON(raw []byte) error {
	jcfg := &jsonConfig{}
	err := json.Unmarshal(raw, jcfg)
	if err != nil {
		logger.Error("Error unmarshaling raft config")
		return err
	}

	cfg.setDefaults()

	// Parse durations. We ignore errors as 0 will take Default values
	// or be caught by Validate().
	waitLeaderTimeout, _ := time.ParseDuration(jcfg.WaitForLeaderTimeout)
	heartbeatTimeout, _ := time.ParseDuration(jcfg.HeartbeatTimeout)
	electionTimeout, _ := time.ParseDuration(jcfg.ElectionTimeout)
	commitTimeout, _ := time.ParseDuration(jcfg.CommitTimeout)
	snapshotInterval, _ := time.ParseDuration(jcfg.SnapshotInterval)
	leaderLeaseTimeout, _ := time.ParseDuration(jcfg.LeaderLeaseTimeout)

	// Set all values in config. For some, take defaults if they are 0.
	// Set values from jcfg if they are not 0 values

	// Own values
	config.SetIfNotDefault(jcfg.DataFolder, &cfg.DataFolder)
	config.SetIfNotDefault(waitForLeader, &cfg.WaitForLeaderTimeout)
	config.CommitRetries = jcfg.CommitRetries

	// Raft values
	config.SetIfNotDefault(heartbeatTimeout, &cfg.HashiraftCfg.HeartbeatTimeout)
	config.SetIfNotDefault(electionTimeout, &cfg.HashiraftCfg.ElectionTimeout)
	config.SetIfNotDefault(commitTimeout, &cfg.HashiraftCfg.CommitTimeout)
	config.SetIfNotDefault(jcfg.MaxAppendEntries, &cfg.HashiraftCfg.MaxAppendEntries)
	config.SetIfNotDefault(jcfg.ShutdownOnRemove, &cfg.HashiraftCfg.ShutdownOnRemove)
	config.SetIfNotDefault(jcfg.TrailingLogs, &cfg.HashiraftCfg.TrailingLogs)
	config.SetIfNotDefault(snapshotInterval, &cfg.HashiraftCfg.SnapshotInterval)
	config.SetIfNotDefault(jcfg.SnapshotThreshold, &cfg.HashiraftCfg.SnapshotThreshold)
	config.SetIfNotDefault(leaderLeaseTimeout, &cfg.HashiraftCfg.LeaderLeaseTimeout)

	return config.Validate()
}

// ToJSON returns the pretty JSON representation of a Config.
func (cfg *Config) ToJSON() ([]byte, error) {
	jcfg := &jsonConfig{}
	jcfg.DataFolder = cfg.DataFolder
	jcfg.WaitForLeaderTimeout = cfg.WaitForLeaderTimeout.String()
	jcfg.CommitRetries = cfg.CommitRetries
	jcfg.HeartbeatTimeout = cfg.HashiraftCfg.HeartbeatTimeout.String()
	jcfg.ElectionTimeout = cfg.HashiraftCfg.ElectionTimeout.String()
	jcfg.CommitTimeout = cfg.HashiraftCfg.CommitTimeout.String()
	jcfg.MaxAppendEntries = cfg.HashiraftCfg.MaxAppendEntries
	jcfg.ShutdownOnRemove = cfg.HashiraftCfg.ShutdownOnRemove
	jcfg.TrailingLogs = cfg.HashiraftCfg.TrailingLogs
	jcfg.SnapshotInterval = cfg.HashiraftCfg.SnapshotInterval.String()
	jcfg.SnapshotThreshold = cfg.HashiraftCfg.SnapshotThreshold
	jcfg.LeaderLeaseTimeout = cfg.HashiraftCfg.LeaderLeaseTimeout.String()

	return config.DefaultJSONMarshal(jcfg)
}

// Default initializes this configuration with working defaults.
func (cfg *Config) Default() error {
	cfg.DataFolder = "" // empty so it gets ommitted
	cfg.WaitForLeaderTimeout = DefaultWaitForLeaderTimeout
	cfg.CommitRetries = DefaultCommitRetries
	cfg.HashiraftCfg = hashiraft.DefaultConfig()

	// These options are imposed over any Default Raft Config.
	// Changing them causes cluster peers to show
	// difficult-to-understand behaviours,
	// usually around the add/remove of peers.
	// That means that changing them will make users wonder why something
	// does not work the way it is expected to.
	// i.e. ShutdownOnRemove will cause that no snapshot will be taken
	// when trying to shutdown a peer after removing it from a cluster.
	cfg.HashiraftCfg.DisableBootstrapAfterElect = false
	cfg.HashiraftCfg.EnableSingleNode = true
	cfg.HashiraftCfg.ShutdownOnRemove = false

	// Set up logging
	cfg.HashiraftCfg.LogOutput = ioutil.Discard
	cfg.HashiraftCfg.Logger = raftStdLogger // see logging.go
	return nil
}

// most defaults come directly from hashiraft.DefaultConfig()
func (cfg *Config) setDefaults() {
	cfg.DataFolder = ""
	cfg.HashiraftCfg = hashiraft.DefaultConfig()

	// These options are imposed over any Default Raft Config.
	// Changing them causes cluster peers to show
	// difficult-to-understand behaviours,
	// usually around the add/remove of peers.
	// That means that changing them will make users wonder why something
	// does not work the way it is expected to.
	// i.e. ShutdownOnRemove will cause that no snapshot will be taken
	// when trying to shutdown a peer after removing it from a cluster.
	cfg.HashiraftCfg.DisableBootstrapAfterElect = false
	cfg.HashiraftCfg.EnableSingleNode = true
	cfg.HashiraftCfg.ShutdownOnRemove = false

	// Set up logging
	cfg.HashiraftCfg.LogOutput = ioutil.Discard
	cfg.HashiraftCfg.Logger = raftStdLogger // see logging.go
}
