// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	protos "github.com/hyperledger/binibft-poc/consensus/binibftprotos"
	"github.com/hyperledger/binibft-poc/consensus/pkg/api"
	"github.com/hyperledger/binibft-poc/consensus/pkg/types"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// Phase indicates the status of the view
type Phase uint8

// These are the different phases
const (
	COMMITTED = iota
	PROPOSED
	PREPARED
	ABORT
)

func (p Phase) String() string {
	switch p {
	case COMMITTED:
		return "COMMITTED"
	case PROPOSED:
		return "PROPOSED"
	case PREPARED:
		return "PREPARED"
	case ABORT:
		return "ABORT"
	default:
		return "Invalid Phase"
	}
}

// State can save and restore the state
//
//go:generate mockery -dir . -name State -case underscore -output ./mocks/
type State interface {
	// Save saves a message.
	Save(message *protos.SavedMessage) error

	// Restore restores the given view to its latest state
	// before a crash, if applicable.
	Restore(*View) error
}

// Comm adds broadcast to the regular comm interface
type Comm interface {
	api.Comm
	BroadcastConsensus(m *protos.Message)
}

type CheckpointRetriever func() (*protos.Proposal, []*protos.Signature)

// View is responsible for running the view protocol
type View struct {
	// Configuration
	DecisionsPerLeader uint64
	RetrieveCheckpoint CheckpointRetriever
	SelfID             uint64
	N                  uint64
	LeaderID           uint64
	Quorum             int
	Number             uint64
	Decider            Decider
	FailureDetector    FailureDetector
	Sync               Synchronizer
	Logger             api.Logger
	Comm               Comm
	Verifier           api.Verifier
	Signer             api.Signer
	MembershipNotifier api.MembershipNotifier
	ProposalSequence   uint64
	DecisionsInView    uint64
	State              State
	Phase              Phase
	InMsgQSize         int

	// Hierarchical consensus configuration
	MyRole      NodeRole
	MySecondary uint64   // For followers, which secondary they belong to
	MyFollowers []uint64 // For secondaries, which followers they manage
	// Runtime
	lastVotedProposalByID map[uint64]*protos.Commit
	incMsgs               chan *incMsg
	myProposalSig         *types.Signature
	inFlightProposal      *types.Proposal
	inFlightRequests      []types.RequestInfo
	lastBroadcastSent     *protos.Message
	// Current sequence sent prepare and commit
	currPrepareSent *protos.Message
	currCommitSent  *protos.Message
	// Prev sequence sent prepare and commit
	// to help lagging replicas catch up
	prevPrepareSent *protos.Message
	prevCommitSent  *protos.Message
	// Current proposal
	prePrepare chan *protos.Message
	prepares   *voteSet
	commits    *voteSet
	// Next proposal
	nextPrePrepare chan *protos.Message
	nextPrepares   *voteSet
	nextCommits    *voteSet

	// Hierarchical consensus vote sets
	secondaryVotes *voteSet // For primary to collect votes from secondaries
	followerVotes  *voteSet // For secondaries to collect votes from followers

	// Flags to prevent duplicate messages
	sentPrepareForSequence bool // To prevent duplicate prepare messages
	sentCommitForSequence  bool // To prevent duplicate commit messages

	beginPrePrepare    time.Time
	MetricsBlacklist   *api.MetricsBlacklist
	MetricsView        *api.MetricsView
	blacklistSupported bool
	abortChan          chan struct{}
	stopOnce           sync.Once
	viewEnded          sync.WaitGroup

	ViewSequences *atomic.Value
}

// Start starts the view
func (v *View) Start() {
	v.stopOnce = sync.Once{}
	v.incMsgs = make(chan *incMsg, v.InMsgQSize)
	v.abortChan = make(chan struct{})
	v.lastVotedProposalByID = make(map[uint64]*protos.Commit)
	v.viewEnded.Add(1)

	v.prePrepare = make(chan *protos.Message, 1)
	v.nextPrePrepare = make(chan *protos.Message, 1)

	// Initialize hierarchical configuration
	v.initializeHierarchicalConfig()

	v.setupVotes()

	go func() {
		v.run()
	}()
}

func (v *View) initializeHierarchicalConfig() {
	// Calculate role dynamically to ensure consistency
	v.updateMyRole()
}

// updateMyRole calculates the current role dynamically using current view state
func (v *View) updateMyRole() {
	nodesList := v.Comm.Nodes()
	v.MyRole = GetNodeRole(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)

	// Update hierarchical relationships based on current role
	switch v.MyRole {
	case FOLLOWER:
		v.MySecondary = GetSecondaryForFollower(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		v.Logger.Infof("Node %d is FOLLOWER under secondary %d", v.SelfID, v.MySecondary)
	case SECONDARY:
		v.MyFollowers = GetFollowersForSecondary(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		v.Logger.Infof("Node %d is SECONDARY managing followers %v", v.SelfID, v.MyFollowers)
	case PRIMARY:
		secondaries := GetSecondaryNodes(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		v.Logger.Infof("Node %d is PRIMARY managing secondaries %v", v.SelfID, secondaries)
	}
}

func (v *View) setupVotes() {
	// Prepares
	acceptPrepares := func(_ uint64, message *protos.Message) bool {
		return message.GetPrepare() != nil
	}

	v.prepares = &voteSet{
		validVote: acceptPrepares,
	}
	v.prepares.clear(v.N)

	v.nextPrepares = &voteSet{
		validVote: acceptPrepares,
	}
	v.nextPrepares.clear(v.N)

	// Commits
	acceptCommits := func(sender uint64, message *protos.Message) bool {
		commit := message.GetCommit()
		if commit == nil {
			return false
		}
		if commit.Signature == nil {
			return false
		}
		// Sender needs to match the inner signature sender
		return commit.Signature.Signer == sender
	}

	v.commits = &voteSet{
		validVote: acceptCommits,
	}
	v.commits.clear(v.N)

	v.nextCommits = &voteSet{
		validVote: acceptCommits,
	}
	v.nextCommits.clear(v.N)

	// Hierarchical vote sets
	if v.MyRole == PRIMARY {
		// Primary collects votes from secondaries
		nodesList := v.Comm.Nodes()
		secondaries := GetSecondaryNodes(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)

		acceptSecondaryVotes := func(sender uint64, message *protos.Message) bool {
			// Check if sender is a secondary
			for _, secondary := range secondaries {
				if sender == secondary {
					return message.GetPrepare() != nil || message.GetCommit() != nil
				}
			}
			return false
		}

		v.secondaryVotes = &voteSet{
			validVote: acceptSecondaryVotes,
		}
		v.secondaryVotes.clear(uint64(len(secondaries)))
	}

	if v.MyRole == SECONDARY {
		// Secondary collects votes from its followers
		acceptFollowerVotes := func(sender uint64, message *protos.Message) bool {
			// Check if sender is one of my followers
			for _, follower := range v.MyFollowers {
				if sender == follower {
					return message.GetPrepare() != nil || message.GetCommit() != nil
				}
			}
			return false
		}

		v.followerVotes = &voteSet{
			validVote: acceptFollowerVotes,
		}
		v.followerVotes.clear(uint64(len(v.MyFollowers)))
	}
}

// HandleMessage handles incoming messages
func (v *View) HandleMessage(sender uint64, m *protos.Message) {
	msg := &incMsg{sender: sender, Message: m}
	select {
	case <-v.abortChan:
		return
	case v.incMsgs <- msg:
	}
}

func (v *View) processMsg(sender uint64, m *protos.Message) {
	if v.Stopped() {
		return
	}
	// Ensure view number is equal to our view
	msgViewNum := viewNumber(m)
	msgProposalSeq := proposalSequence(m)

	if msgViewNum != v.Number {
		v.Logger.Warnf("%d got message %v from %d of view %d, expected view %d", v.SelfID, m, sender, msgViewNum, v.Number)
		if sender != v.LeaderID {
			v.discoverIfSyncNeeded(sender, m)
			return
		}
		v.FailureDetector.Complain(v.Number, false)
		// Else, we got a message with a wrong view from the leader.
		if msgViewNum > v.Number {
			v.Sync.Sync()
		}
		v.stop()
		return
	}

	if msgProposalSeq == v.ProposalSequence-1 && v.ProposalSequence > 0 {
		v.handlePrevSeqMessage(msgProposalSeq, sender, m)
		return
	}

	v.Logger.Debugf("%d got message %s from %d with seq %d", v.SelfID, MsgToString(m), sender, msgProposalSeq)
	// This message is either for this proposal or the next one (we might be behind the rest)
	if msgProposalSeq != v.ProposalSequence && msgProposalSeq != v.ProposalSequence+1 {
		v.Logger.Warnf("%d got message from %d with sequence %d but our sequence is %d", v.SelfID, sender, msgProposalSeq, v.ProposalSequence)
		v.discoverIfSyncNeeded(sender, m)
		return
	}

	msgForNextProposal := msgProposalSeq == v.ProposalSequence+1

	if pp := m.GetPrePrepare(); pp != nil {
		v.processPrePrepare(pp, m, msgForNextProposal, sender)
		return
	}

	// Else, it's a prepare or a commit.
	// Ignore votes from ourselves.
	if sender == v.SelfID {
		return
	}

	if prp := m.GetPrepare(); prp != nil {
		v.processHierarchicalPrepare(sender, m, msgForNextProposal)
		return
	}

	if cmt := m.GetCommit(); cmt != nil {
		v.processHierarchicalCommit(sender, m, msgForNextProposal)
		return
	}
}

func (v *View) run() {
	defer v.viewEnded.Done()
	defer func() {
		v.ViewSequences.Store(ViewSequence{
			ProposalSeq: v.ProposalSequence,
			ViewActive:  false,
		})
	}()
	for {
		select {
		case <-v.abortChan:
			return
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
			// Always call doPhase after processing a message to ensure phase transitions
			v.doPhase()
		default:
			v.doPhase()
		}
	}
}

func (v *View) doPhase() {
	v.Logger.Infof("Node %d doPhase() called in phase %s, role=%v", v.SelfID, v.Phase.String(), v.MyRole)
	switch v.Phase {
	case PROPOSED:
		// Send hierarchically instead of broadcasting to all nodes
		v.sendHierarchically(v.lastBroadcastSent)
		v.Logger.Infof("Node %d calling processPrepares()", v.SelfID)
		v.Phase = v.processPrepares()
		v.Logger.Infof("Node %d transitioned to phase %s", v.SelfID, v.Phase.String())
	case PREPARED:
		// Send hierarchically instead of broadcasting to all nodes
		v.sendHierarchically(v.lastBroadcastSent)
		v.Logger.Infof("Node %d calling prepared() method", v.SelfID)
		v.Phase = v.prepared()
		v.Logger.Infof("Node %d transitioned to phase %s", v.SelfID, v.Phase.String())
	case COMMITTED:
		v.Phase = v.processProposal()
		v.Logger.Infof("Node %d transitioned to phase %s", v.SelfID, v.Phase.String())
	case ABORT:
		v.Logger.Infof("Node %d doPhase() returning due to ABORT", v.SelfID)
		return
	default:
		v.Logger.Panicf("Unknown phase in view : %v", v)
	}

	v.MetricsView.Phase.Set(float64(v.Phase))
}

// sendHierarchically sends messages according to hierarchical consensus rules
func (v *View) sendHierarchically(msg *protos.Message) {
	if msg == nil {
		return
	}

	// Determine message type and send accordingly
	if msg.GetPrepare() != nil {
		v.sendPrepareHierarchically(msg)
	} else if msg.GetCommit() != nil {
		v.sendCommitHierarchically(msg)
	} else {
		// For other message types (like PrePrepare), use existing hierarchical logic
		// or fall back to broadcast only for specific roles
		switch v.MyRole {
		case PRIMARY:
			// Primary can broadcast certain message types like PrePrepare to secondaries
			nodesList := v.Comm.Nodes()
			secondaries := GetSecondaryNodes(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
			for _, secondary := range secondaries {
				v.Comm.SendConsensus(secondary, msg)
			}
		case SECONDARY, FOLLOWER:
			// Secondaries and followers should not broadcast, they send to specific targets
			v.Logger.Debugf("Node %d (role %s) not broadcasting message type %T", v.SelfID, v.MyRole.String(), msg.GetContent())
		}
	}
}

// sendPrepareHierarchically sends prepare messages according to hierarchical rules
func (v *View) sendPrepareHierarchically(prepareMsg *protos.Message) {
	switch v.MyRole {
	case PRIMARY:
		// Primary registers its own prepare vote directly (only in prepares, not secondaryVotes)
		v.Logger.Infof("PRIMARY %d registering its own prepare vote", v.SelfID)
		if v.prepares != nil {
			v.prepares.registerVote(v.SelfID, prepareMsg)
		}
	case SECONDARY:
		// Secondary sends prepare to primary and other secondaries after collecting from followers
		nodesList := v.Comm.Nodes()

		// Send to PRIMARY
		primary := GetPrimaryNode(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		v.Logger.Infof("SECONDARY %d sending prepare to primary %d", v.SelfID, primary)
		v.Comm.SendConsensus(primary, prepareMsg)

		// Send to OTHER SECONDARIES
		allSecondaries := GetSecondaryNodes(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		for _, secondary := range allSecondaries {
			if secondary != v.SelfID { // Don't send to self
				v.Logger.Infof("SECONDARY %d sending prepare to secondary %d", v.SelfID, secondary)
				v.Comm.SendConsensus(secondary, prepareMsg)
			}
		}
	case FOLLOWER:
		// Follower sends prepare to other followers in the same group
		nodesList := v.Comm.Nodes()
		mySecondary := GetSecondaryForFollower(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		followersInMyGroup := GetFollowersForSecondary(mySecondary, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)

		// Filter out self from followers list
		otherFollowers := make([]uint64, 0)
		for _, follower := range followersInMyGroup {
			if follower != v.SelfID {
				otherFollowers = append(otherFollowers, follower)
			}
		}

		if len(otherFollowers) > 0 {
			// Send to other followers in the group
			v.Logger.Infof("FOLLOWER %d sending prepare to other followers %v in secondary %d's group", v.SelfID, otherFollowers, mySecondary)
			for _, follower := range otherFollowers {
				v.Comm.SendConsensus(follower, prepareMsg)
			}
		} else {
			// If no other followers, send directly to secondary
			v.Logger.Infof("FOLLOWER %d is alone in group, sending prepare directly to secondary %d", v.SelfID, mySecondary)
			v.Comm.SendConsensus(mySecondary, prepareMsg)
		}
	}
}

func (v *View) processPrePrepare(pp *protos.PrePrepare, m *protos.Message, msgForNextProposal bool, sender uint64) {
	if pp.Proposal == nil {
		v.Logger.Warnf("%d got pre-prepare from %d with empty proposal", v.SelfID, sender)
		return
	}

	// Use the role that was calculated during view initialization to ensure consistency
	nodesList := v.Comm.Nodes()

	// Hierarchical validation: Only accept PrePrepare from appropriate sender
	switch v.MyRole {
	case PRIMARY:
		// Primary can receive from itself
		if sender != v.SelfID {
			v.Logger.Warnf("PRIMARY %d got pre-prepare from %d, but primary should only receive from itself", v.SelfID, sender)
			return
		}
	case SECONDARY:
		// Secondary should receive from Primary
		primary := GetPrimaryNode(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		if sender != primary {
			v.Logger.Warnf("SECONDARY %d got pre-prepare from %d, but should only receive from primary %d", v.SelfID, sender, primary)
			return
		}
	case FOLLOWER:
		// Follower should receive from its Secondary
		mySecondary := GetSecondaryForFollower(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		if sender != mySecondary {
			v.Logger.Warnf("FOLLOWER %d got pre-prepare from %d, but should only receive from secondary %d", v.SelfID, sender, mySecondary)
			return
		}
	}

	prePrepareChan := v.prePrepare
	currentOrNext := "current"

	if msgForNextProposal {
		prePrepareChan = v.nextPrePrepare
		currentOrNext = "next"
	}

	select {
	case prePrepareChan <- m:
		// Forward PrePrepare in hierarchical manner
		v.forwardPrePrepareHierarchically(m)
	default:
		v.Logger.Warnf("Got a pre-prepare for %s sequence without processing previous one, dropping message", currentOrNext)
	}
}

func (v *View) forwardPrePrepareHierarchically(m *protos.Message) {
	switch v.MyRole {
	case SECONDARY:
		// Secondary forwards to its followers
		v.Logger.Infof("SECONDARY %d forwarding PrePrepare to followers %v", v.SelfID, v.MyFollowers)
		for _, follower := range v.MyFollowers {
			v.Comm.SendConsensus(follower, m)
		}
	case PRIMARY, FOLLOWER:
		// Primary and Followers don't forward PrePrepare
		// Primary already sent to secondaries, Followers are leaf nodes
	}
}

// sendCommitHierarchically sends commit votes in hierarchical manner
func (v *View) sendCommitHierarchically(commitMsg *protos.Message) {
	switch v.MyRole {
	case PRIMARY:
		// Primary registers its own commit vote directly (only in commits, not secondaryVotes)
		v.Logger.Infof("PRIMARY %d registering its own commit vote", v.SelfID)
		if v.commits != nil {
			v.commits.registerVote(v.SelfID, commitMsg)
		}
	case SECONDARY:
		// Secondary sends its commit vote to the primary and other secondaries
		nodesList := v.Comm.Nodes()

		// Send to PRIMARY
		primary := GetPrimaryNode(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		v.Logger.Infof("SECONDARY %d sending commit to primary %d", v.SelfID, primary)
		v.Comm.SendConsensus(primary, commitMsg)

		// Send to OTHER SECONDARIES
		allSecondaries := GetSecondaryNodes(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		for _, secondary := range allSecondaries {
			if secondary != v.SelfID { // Don't send to self
				v.Logger.Infof("SECONDARY %d sending commit to secondary %d", v.SelfID, secondary)
				v.Comm.SendConsensus(secondary, commitMsg)
			}
		}
	case FOLLOWER:
		// Follower sends commit to other followers in the same group, or to secondary if alone
		nodesList := v.Comm.Nodes()
		mySecondary := GetSecondaryForFollower(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		followersInMyGroup := GetFollowersForSecondary(mySecondary, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)

		// Filter out self from followers list
		otherFollowers := make([]uint64, 0)
		for _, follower := range followersInMyGroup {
			if follower != v.SelfID {
				otherFollowers = append(otherFollowers, follower)
			}
		}

		if len(otherFollowers) > 0 {
			// Send to other followers in the group
			v.Logger.Infof("FOLLOWER %d sending commit to other followers %v in secondary %d's group", v.SelfID, otherFollowers, mySecondary)
			for _, follower := range otherFollowers {
				v.Comm.SendConsensus(follower, commitMsg)
			}
		} else {
			// If no other followers, send directly to secondary
			v.Logger.Infof("FOLLOWER %d is alone in group, sending commit directly to secondary %d", v.SelfID, mySecondary)
			v.Comm.SendConsensus(mySecondary, commitMsg)
		}
	}
}

// sendHierarchicalHeartbeat sends heartbeats in hierarchical manner
func (v *View) sendHierarchicalHeartbeat() {
	heartbeat := &protos.Message{
		Content: &protos.Message_HeartBeat{
			HeartBeat: &protos.HeartBeat{
				View: v.Number,
				Seq:  v.ProposalSequence,
			},
		},
	}

	switch v.MyRole {
	case PRIMARY:
		// Primary sends heartbeats to secondaries
		nodesList := v.Comm.Nodes()
		secondaries := GetSecondaryNodes(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		v.Logger.Debugf("PRIMARY %d sending heartbeat to secondaries %v", v.SelfID, secondaries)
		for _, secondary := range secondaries {
			v.Comm.SendConsensus(secondary, heartbeat)
		}
	case SECONDARY:
		// Secondary sends heartbeats to its followers
		v.Logger.Debugf("SECONDARY %d sending heartbeat to followers %v", v.SelfID, v.MyFollowers)
		for _, follower := range v.MyFollowers {
			v.Comm.SendConsensus(follower, heartbeat)
		}
	case FOLLOWER:
		// Followers don't send heartbeats
	}
}

func (v *View) processHierarchicalPrepare(sender uint64, m *protos.Message, msgForNextProposal bool) {
	// Use the role that was calculated during view initialization to ensure consistency
	nodesList := v.Comm.Nodes()

	// Hierarchical consensus logic
	switch v.MyRole {
	case PRIMARY:
		// Primary receives prepare votes from secondaries
		if v.secondaryVotes != nil {
			v.secondaryVotes.registerVote(sender, m)
		}
		// Also register in normal prepare set for backward compatibility
		if msgForNextProposal {
			v.nextPrepares.registerVote(sender, m)
		} else {
			v.prepares.registerVote(sender, m)
		}

	case SECONDARY:
		// Secondary receives prepare votes from followers
		if v.followerVotes != nil {
			v.followerVotes.registerVote(sender, m)
		}
		// Check if we have majority from followers, then send prepare to other secondaries and primary
		v.checkFollowerMajorityAndSendPrepare(m, msgForNextProposal)

	case FOLLOWER:
		// Follower sends prepare to other followers in the same group, or to secondary if alone
		mySecondary := GetSecondaryForFollower(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		followersInMyGroup := GetFollowersForSecondary(mySecondary, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)

		// Filter out self from followers list
		otherFollowers := make([]uint64, 0)
		for _, follower := range followersInMyGroup {
			if follower != v.SelfID {
				otherFollowers = append(otherFollowers, follower)
			}
		}

		if len(otherFollowers) > 0 {
			// Send to other followers in the group
			v.Logger.Infof("FOLLOWER %d sending prepare to other followers %v in secondary %d's group", v.SelfID, otherFollowers, mySecondary)
			for _, follower := range otherFollowers {
				v.Comm.SendConsensus(follower, m)
			}
		} else {
			// If no other followers, send directly to secondary
			v.Logger.Infof("FOLLOWER %d is alone in group, sending prepare directly to secondary %d", v.SelfID, mySecondary)
			v.Comm.SendConsensus(mySecondary, m)
		}
	}
}

func (v *View) processHierarchicalCommit(sender uint64, m *protos.Message, msgForNextProposal bool) {
	// Get current role dynamically
	nodesList := v.Comm.Nodes()
	currentRole := GetNodeRole(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)

	// Hierarchical consensus logic
	switch currentRole {
	case PRIMARY:
		// Primary receives commit votes from secondaries
		v.Logger.Infof("PRIMARY %d received commit from %d", v.SelfID, sender)
		if v.secondaryVotes != nil {
			v.secondaryVotes.registerVote(sender, m)
		}
		// Also register in normal commit set
		if msgForNextProposal {
			v.nextCommits.registerVote(sender, m)
		} else {
			v.commits.registerVote(sender, m)
		}

	case SECONDARY:
		// Secondary receives commit votes from followers and other secondaries
		nodesList := v.Comm.Nodes()
		allSecondaries := GetSecondaryNodes(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)

		// Check if sender is another secondary
		isFromSecondary := false
		for _, secondary := range allSecondaries {
			if sender == secondary && sender != v.SelfID {
				isFromSecondary = true
				break
			}
		}

		if isFromSecondary {
			// Register commit from other secondary
			if v.secondaryVotes != nil {
				v.secondaryVotes.registerVote(sender, m)
			}
			// Also register in normal commit set
			if msgForNextProposal {
				v.nextCommits.registerVote(sender, m)
			} else {
				v.commits.registerVote(sender, m)
			}
		} else {
			// Register commit from follower
			if v.followerVotes != nil {
				v.followerVotes.registerVote(sender, m)
			}
			// Check if we have majority from followers, then forward to primary
			v.checkFollowerMajorityAndForward(m, msgForNextProposal)
		}

	case FOLLOWER:
		// Follower sends commit to other followers in the same group, or to secondary if alone
		mySecondary := GetSecondaryForFollower(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		followersInMyGroup := GetFollowersForSecondary(mySecondary, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)

		// Filter out self from followers list
		otherFollowers := make([]uint64, 0)
		for _, follower := range followersInMyGroup {
			if follower != v.SelfID {
				otherFollowers = append(otherFollowers, follower)
			}
		}

		if len(otherFollowers) > 0 {
			// Send to other followers in the group
			v.Logger.Infof("FOLLOWER %d sending commit to other followers %v in secondary %d's group", v.SelfID, otherFollowers, mySecondary)
			for _, follower := range otherFollowers {
				v.Comm.SendConsensus(follower, m)
			}
		} else {
			// If no other followers, send directly to secondary
			v.Logger.Infof("FOLLOWER %d is alone in group, sending commit directly to secondary %d", v.SelfID, mySecondary)
			v.Comm.SendConsensus(mySecondary, m)
		}
	}
}

// checkFollowerMajorityAndSendPrepare - SECONDARY sends prepare to other secondaries and primary when it has majority from followers
func (v *View) checkFollowerMajorityAndSendPrepare(m *protos.Message, msgForNextProposal bool) {
	// Prevent duplicate prepare messages for the same sequence
	if v.sentPrepareForSequence {
		return
	}

	nodesList := v.Comm.Nodes()

	// If no followers, send prepare immediately to other secondaries and primary
	if len(v.MyFollowers) == 0 {
		v.sendPrepareToSecondariesAndPrimary(nodesList)
		v.sentPrepareForSequence = true
		return
	}

	// Calculate required majority from followers
	requiredVotes := (len(v.MyFollowers) / 2) + 1
	currentVotes := len(v.followerVotes.voted)

	v.Logger.Debugf("SECONDARY %d has %d prepare votes from followers, needs %d", v.SelfID, currentVotes, requiredVotes)

	if currentVotes >= requiredVotes {
		// We have majority from followers, send prepare to other secondaries and primary
		v.sendPrepareToSecondariesAndPrimary(nodesList)
		v.sentPrepareForSequence = true

		// Clear votes to avoid duplicate sending
		v.followerVotes.clear(uint64(len(v.MyFollowers)))
	}
}

// sendPrepareToSecondariesAndPrimary - SECONDARY sends its prepare vote to other secondaries and primary
func (v *View) sendPrepareToSecondariesAndPrimary(nodesList []uint64) {
	// Check if we have a proposal to create prepare for
	if v.inFlightProposal == nil {
		v.Logger.Warnf("SECONDARY %d cannot send prepare: no inFlightProposal", v.SelfID)
		return
	}

	// Create prepare message for this secondary
	prepareMsg := &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{
				View:   v.Number,
				Seq:    v.ProposalSequence,
				Digest: v.inFlightProposal.Digest(),
			},
		},
	}

	// Send to PRIMARY
	primary := GetPrimaryNode(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
	v.Logger.Infof("SECONDARY %d sending prepare to primary %d", v.SelfID, primary)
	v.Comm.SendConsensus(primary, prepareMsg)

	// Send to OTHER SECONDARIES
	allSecondaries := GetSecondaryNodes(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
	for _, secondary := range allSecondaries {
		if secondary != v.SelfID { // Don't send to self
			v.Logger.Infof("SECONDARY %d sending prepare to secondary %d", v.SelfID, secondary)
			v.Comm.SendConsensus(secondary, prepareMsg)
		}
	}
}

// checkFollowerMajorityAndForward - SECONDARY forwards commit messages to primary when it has majority from followers
func (v *View) checkFollowerMajorityAndForward(m *protos.Message, msgForNextProposal bool) {
	if v.followerVotes == nil || len(v.MyFollowers) == 0 {
		return
	}

	// Calculate required majority from followers
	requiredVotes := (len(v.MyFollowers) / 2) + 1
	currentVotes := len(v.followerVotes.voted)

	v.Logger.Debugf("SECONDARY %d has %d commit votes from followers, needs %d", v.SelfID, currentVotes, requiredVotes)

	if currentVotes >= requiredVotes {
		// We have majority from followers, forward commit to primary
		nodesList := v.Comm.Nodes()
		primary := GetPrimaryNode(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		v.Logger.Infof("SECONDARY %d has majority from followers, forwarding commit to primary %d", v.SelfID, primary)
		v.Comm.SendConsensus(primary, m)

		// Send hierarchical heartbeat after forwarding to primary
		v.sendHierarchicalHeartbeat()

		// Clear votes to avoid duplicate forwarding
		v.followerVotes.clear(uint64(len(v.MyFollowers)))
	}
}

func (v *View) prepared() Phase {
	v.Logger.Infof("Node %d prepared() called, role=%v", v.SelfID, v.MyRole)
	proposal := v.inFlightProposal
	if proposal == nil {
		v.Logger.Warnf("prepared called but inFlightProposal is nil, returning ABORT")
		return ABORT
	}
	v.Logger.Infof("Node %d calling processCommits", v.SelfID)
	signatures, phase := v.processCommits(proposal)
	if phase == ABORT {
		v.Logger.Warnf("Node %d processCommits returned ABORT", v.SelfID)
		return ABORT
	}
	v.Logger.Infof("Node %d processCommits completed successfully", v.SelfID)

	seq := v.ProposalSequence

	v.Logger.Infof("%d processed commits for proposal with seq %d", v.SelfID, seq)

	v.MetricsView.CountBatchAll.Add(1)
	v.MetricsView.CountTxsAll.Add(float64(len(v.inFlightRequests)))
	size := 0
	size += len(proposal.Metadata) + len(proposal.Header) + len(proposal.Payload)
	for i := range signatures {
		size += len(signatures[i].Value) + len(signatures[i].Msg)
	}
	v.MetricsView.SizeOfBatch.Add(float64(size))
	v.MetricsView.LatencyBatchProcessing.Observe(time.Since(v.beginPrePrepare).Seconds())

	v.decide(proposal, signatures, v.inFlightRequests)
	return COMMITTED
}

func (v *View) processProposal() Phase {
	v.prevPrepareSent = v.currPrepareSent
	v.prevCommitSent = v.currCommitSent
	v.currPrepareSent = nil
	v.currCommitSent = nil
	v.inFlightProposal = nil
	v.inFlightRequests = nil
	v.lastBroadcastSent = nil

	var proposal types.Proposal
	var receivedProposal *protos.Message
	var prevCommits []*protos.Signature

	var gotPrePrepare bool
	for !gotPrePrepare {
		select {
		case <-v.abortChan:
			return ABORT
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
		case msg := <-v.prePrepare:
			gotPrePrepare = true
			receivedProposal = msg
			prePrepare := msg.GetPrePrepare()
			prop := prePrepare.Proposal
			prevCommits = prePrepare.PrevCommitSignatures
			proposal = types.Proposal{
				VerificationSequence: int64(prop.VerificationSequence),
				Metadata:             prop.Metadata,
				Payload:              prop.Payload,
				Header:               prop.Header,
			}
		}
	}

	requests, err := v.verifyProposal(proposal, prevCommits)
	if err != nil {
		v.Logger.Warnf("%d received bad proposal from %d: %v", v.SelfID, v.LeaderID, err)
		v.FailureDetector.Complain(v.Number, false)
		v.Sync.Sync()
		v.stop()
		return ABORT
	}

	v.MetricsView.CountTxsInBatch.Set(float64(len(requests)))
	v.beginPrePrepare = time.Now()

	seq := v.ProposalSequence

	prepareMessage := v.createPrepare(seq, proposal)

	// We are about to send a prepare for a pre-prepare,
	// so we record the pre-prepare.
	savedMsg := &protos.SavedMessage{
		Content: &protos.SavedMessage_ProposedRecord{
			ProposedRecord: &protos.ProposedRecord{
				PrePrepare: receivedProposal.GetPrePrepare(),
				Prepare:    prepareMessage.GetPrepare(),
			},
		},
	}
	if err = v.State.Save(savedMsg); err != nil {
		v.Logger.Panicf("Failed to save message to state, error: %v", err)
	}
	v.lastBroadcastSent = prepareMessage
	v.currPrepareSent = proto.Clone(prepareMessage).(*protos.Message)
	v.currPrepareSent.GetPrepare().Assist = true
	v.inFlightProposal = &proposal
	v.inFlightRequests = requests

	if v.SelfID == v.LeaderID {
		// In hierarchical consensus, primary sends PrePrepare only to secondaries
		v.sendHierarchically(receivedProposal)
	}

	v.Logger.Infof("Processed proposal with seq %d", seq)
	return PROPOSED
}

func (v *View) createPrepare(seq uint64, proposal types.Proposal) *protos.Message {
	return &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{
				Seq:    seq,
				View:   v.Number,
				Digest: proposal.Digest(),
			},
		},
	}
}

func (v *View) processPrepares() Phase {
	v.Logger.Infof("Node %d processPrepares() called, role=%v", v.SelfID, v.MyRole)
	proposal := v.inFlightProposal
	if proposal == nil {
		v.Logger.Warnf("processPrepares called but inFlightProposal is nil, returning ABORT")
		return ABORT
	}
	expectedDigest := proposal.Digest()

	var voterIDs []uint64
	requiredVotes := v.getRequiredPreparesToProceed()

	v.Logger.Infof("Node %d processPrepares: requiredVotes=%d, role=%v", v.SelfID, requiredVotes, v.MyRole)

	for len(voterIDs) < requiredVotes {
		v.Logger.Infof("Node %d waiting for prepares: have %d, need %d", v.SelfID, len(voterIDs), requiredVotes)
		select {
		case <-v.abortChan:
			return ABORT
		case msg := <-v.incMsgs:
			v.Logger.Infof("Node %d processing incoming message in processPrepares from %d", v.SelfID, msg.sender)
			v.processMsg(msg.sender, msg.Message)
		case vote := <-v.getPreparesChannel():
			if vote == nil || vote.Message == nil {
				v.Logger.Debugf("Node %d received nil prepare vote", v.SelfID)
				continue
			}
			prepare := vote.GetPrepare()
			if prepare == nil {
				v.Logger.Debugf("Node %d received vote with nil prepare", v.SelfID)
				continue
			}
			if prepare.Digest != expectedDigest {
				seq := v.ProposalSequence
				v.Logger.Warnf("Got wrong digest at processPrepares for prepare with seq %d, expecting %v but got %v, we are in seq %d", prepare.Seq, expectedDigest, prepare.Digest, seq)
				continue
			}
			v.Logger.Infof("Node %d accepted prepare vote from %d", v.SelfID, vote.sender)
			voterIDs = append(voterIDs, vote.sender)
		}
	}

	v.Logger.Infof("%d collected %d prepares from %v", v.SelfID, len(voterIDs), voterIDs)

	// SignProposal returns a types.Signature with the following 3 fields:
	// ID: The integer that represents this node.
	// Value: The signature, encoded according to the specific signature specification.
	// Msg: A succinct representation of the proposal that binds this proposal unequivocally.

	// The block proof consists of the aggregation of all these signatures from 2f+1 commits of different nodes.

	prpFrom := &protos.PreparesFrom{
		Ids: voterIDs,
	}

	prpFromRaw, err := proto.Marshal(prpFrom)
	if err != nil {
		v.Logger.Panicf("Failed marshaling prepares from: %v", err)
	}

	v.myProposalSig = v.Signer.SignProposal(*proposal, prpFromRaw)

	seq := v.ProposalSequence

	commitMsg := &protos.Message{
		Content: &protos.Message_Commit{
			Commit: &protos.Commit{
				View:   v.Number,
				Digest: expectedDigest,
				Seq:    seq,
				Signature: &protos.Signature{
					Signer: v.myProposalSig.ID,
					Value:  v.myProposalSig.Value,
					Msg:    v.myProposalSig.Msg,
				},
			},
		},
	}

	preparedProof := &protos.SavedMessage{
		Content: &protos.SavedMessage_Commit{
			Commit: commitMsg,
		},
	}

	// We received enough prepares to send a commit.
	// Save the commit message we are about to send.
	if err = v.State.Save(preparedProof); err != nil {
		v.Logger.Panicf("Failed to save message to state, error: %v", err)
	}
	v.currCommitSent = proto.Clone(commitMsg).(*protos.Message)
	v.currCommitSent.GetCommit().Assist = true
	v.lastBroadcastSent = commitMsg

	// In hierarchical consensus, send commit vote hierarchically
	v.sendCommitHierarchically(commitMsg)

	v.Logger.Infof("Processed prepares for proposal with seq %d, transitioning to PREPARED phase", seq)
	return PREPARED
}

func (v *View) processCommits(proposal *types.Proposal) ([]types.Signature, Phase) {
	var signatures []types.Signature

	signatureCollector := &voteVerifier{
		validVotes:     make(chan types.Signature, cap(v.getCommitsChannel())),
		expectedDigest: proposal.Digest(),
		proposal:       proposal,
		v:              v,
	}

	var voterIDs []uint64
	requiredCommits := v.getRequiredCommitsToProceed()

	v.Logger.Infof("Node %d processCommits: requiredCommits=%d, role=%v", v.SelfID, requiredCommits, v.MyRole)

	for len(signatures) < requiredCommits {
		v.Logger.Infof("Node %d waiting for commits: have %d, need %d", v.SelfID, len(signatures), requiredCommits)
		select {
		case <-v.abortChan:
			v.Logger.Infof("Node %d processCommits aborted", v.SelfID)
			return nil, ABORT
		case msg := <-v.incMsgs:
			v.Logger.Infof("Node %d processing incoming message in processCommits from %d", v.SelfID, msg.sender)
			v.processMsg(msg.sender, msg.Message)
		case vote := <-v.getCommitsChannel():
			if vote == nil || vote.Message == nil {
				v.Logger.Debugf("Node %d received nil vote in processCommits", v.SelfID)
				continue
			}
			v.Logger.Infof("Node %d received commit vote from %d", v.SelfID, vote.sender)
			// Valid votes end up written into the 'validVotes' channel.
			go func(vote *protos.Message, sender uint64) {
				v.Logger.Infof("Node %d verifying commit from %d", v.SelfID, sender)
				signatureCollector.verifyVote(vote)
			}(vote.Message, vote.sender)
		case signature := <-signatureCollector.validVotes:
			v.Logger.Infof("Node %d accepted valid commit signature from %d", v.SelfID, signature.ID)
			signatures = append(signatures, signature)
			voterIDs = append(voterIDs, signature.ID)
		}
	}

	v.Logger.Infof("%d collected %d commits from %v", v.SelfID, len(signatures), voterIDs)

	return signatures, COMMITTED
}

func (v *View) verifyProposal(proposal types.Proposal, prevCommits []*protos.Signature) ([]types.RequestInfo, error) {
	// Verify proposal has correct structure and contains authorized requests.
	requests, err := v.Verifier.VerifyProposal(proposal)
	if err != nil {
		v.Logger.Warnf("Received bad proposal: %v", err)
		return nil, err
	}

	// Verify proposal's metadata is valid.
	md := &protos.ViewMetadata{}
	if err = proto.Unmarshal(proposal.Metadata, md); err != nil {
		return nil, err
	}

	if md.ViewId != v.Number {
		v.Logger.Warnf("Expected view number %d but got %d", v.Number, md.ViewId)
		return nil, errors.New("invalid view number")
	}

	if md.LatestSequence != v.ProposalSequence {
		v.Logger.Warnf("Expected proposal sequence %d but got %d", v.ProposalSequence, md.LatestSequence)
		return nil, errors.New("invalid proposal sequence")
	}

	if md.DecisionsInView != v.DecisionsInView {
		v.Logger.Warnf("Expected decisions in view %d but got %d", v.DecisionsInView, md.DecisionsInView)
		return nil, errors.New("invalid decisions in view")
	}

	expectedSeq := v.Verifier.VerificationSequence()
	if uint64(proposal.VerificationSequence) != expectedSeq {
		v.Logger.Warnf("Expected verification sequence %d but got %d", expectedSeq, proposal.VerificationSequence)
		return nil, errors.New("verification sequence mismatch")
	}

	prepareAcknowledgements, err := v.verifyPrevCommitSignatures(prevCommits, expectedSeq)
	if err != nil {
		return nil, err
	}

	if err = v.verifyBlacklist(prevCommits, expectedSeq, md.BlackList, prepareAcknowledgements); err != nil {
		return nil, err
	}

	// Check that the metadata contains a digest of the previous commit signatures
	prevCommitDigest := CommitSignaturesDigest(prevCommits)
	if !bytes.Equal(prevCommitDigest, md.PrevCommitSignatureDigest) && v.DecisionsPerLeader > 0 {
		return nil, errors.Errorf("prev commit signatures received from leader mismatches the metadata digest")
	}

	return requests, nil
}

func (v *View) verifyPrevCommitSignatures(prevCommitSignatures []*protos.Signature, currVerificationSeq uint64) (map[uint64]*protos.PreparesFrom, error) {
	prevPropRaw, _ := v.RetrieveCheckpoint()
	prevProposalMetadata := &protos.ViewMetadata{}
	if err := proto.Unmarshal(prevPropRaw.Metadata, prevProposalMetadata); err != nil {
		v.Logger.Panicf("Couldn't unmarshal the previous persisted proposal metadata: %v", err)
	}

	v.Logger.Debugf("Previous proposal verification sequence: %d, current verification sequence: %d", prevPropRaw.VerificationSequence, currVerificationSeq)
	if prevPropRaw.VerificationSequence != currVerificationSeq {
		v.Logger.Infof("Skipping verifying prev commit signatures due to verification sequence advancing from %d to %d",
			prevPropRaw.VerificationSequence, currVerificationSeq)
		return nil, nil
	}

	prepareAcknowledgements := make(map[uint64]*protos.PreparesFrom)

	prevProp := types.Proposal{
		VerificationSequence: int64(prevPropRaw.VerificationSequence),
		Metadata:             prevPropRaw.Metadata,
		Payload:              prevPropRaw.Payload,
		Header:               prevPropRaw.Header,
	}

	// All previous commit signatures should be verifiable
	for _, sig := range prevCommitSignatures {
		aux, err := v.Verifier.VerifyConsenterSig(types.Signature{
			ID:    sig.Signer,
			Msg:   sig.Msg,
			Value: sig.Value,
		}, prevProp)
		if err != nil {
			return nil, errors.Errorf("failed verifying consenter signature of %d: %v", sig.Signer, err)
		}
		prpf := &protos.PreparesFrom{}
		if err = proto.Unmarshal(aux, prpf); err != nil {
			return nil, errors.Errorf("failed unmarshaling auxiliary input from %d: %v", sig.Signer, err)
		}
		prepareAcknowledgements[sig.Signer] = prpf
	}

	return prepareAcknowledgements, nil
}

func (v *View) verifyBlacklist(prevCommitSignatures []*protos.Signature, currVerificationSeq uint64, pendingBlacklist []uint64, prepareAcknowledgements map[uint64]*protos.PreparesFrom) error {
	if v.DecisionsPerLeader == 0 {
		v.Logger.Debugf("DecisionsPerLeader is 0, hence leader rotation is inactive")
		if len(pendingBlacklist) > 0 {
			v.Logger.Warnf("Blacklist cannot be non-empty (%v) if rotation is inactive", pendingBlacklist)
			return errors.Errorf("rotation is inactive but blacklist is not empty: %v", pendingBlacklist)
		}
		return nil
	}

	prevPropRaw, myLastCommitSignatures := v.RetrieveCheckpoint()
	prevProposalMetadata := &protos.ViewMetadata{}
	if err := proto.Unmarshal(prevPropRaw.Metadata, prevProposalMetadata); err != nil {
		v.Logger.Panicf("Couldn't unmarshal the previous persisted proposal metadata: %v", err)
	}

	v.Logger.Debugf("Previous proposal verification sequence: %d, current verification sequence: %d", prevPropRaw.VerificationSequence, currVerificationSeq)
	if prevPropRaw.VerificationSequence != currVerificationSeq {
		// If there has been a reconfiguration, black list should remain the same
		if !equalIntLists(prevProposalMetadata.BlackList, pendingBlacklist) {
			return errors.Errorf("blacklist changed (%v --> %v) during reconfiguration", prevProposalMetadata.BlackList, pendingBlacklist)
		}
		v.Logger.Infof("Skipping verifying prev commits due to verification sequence advancing from %d to %d",
			prevPropRaw.VerificationSequence, currVerificationSeq)
		return nil
	}

	if v.MembershipNotifier != nil && v.MembershipNotifier.MembershipChange() {
		// If there has been a membership change, black list should remain the same
		if !equalIntLists(prevProposalMetadata.BlackList, pendingBlacklist) {
			return errors.Errorf("blacklist changed (%v --> %v) during membership change", prevProposalMetadata.BlackList, pendingBlacklist)
		}
		v.Logger.Infof("Skipping verifying prev commits due to membership change")
		return nil
	}

	_, f := computeQuorum(v.N)

	if v.blacklistingSupported(f, myLastCommitSignatures) && len(prevCommitSignatures) < len(myLastCommitSignatures) {
		return errors.Errorf("only %d out of %d required previous commits is included in pre-prepare",
			len(prevCommitSignatures), len(myLastCommitSignatures))
	}

	// We previously verified the previous commit signatures, now we need to ensure that the blacklist
	// of this proposal is obtained by applying the deterministic blacklist maintenance algorithm
	// on the blacklist of the previous proposal which has been committed.

	blacklist := &blacklist{
		currentLeader:      v.LeaderID,
		leaderRotation:     v.DecisionsPerLeader > 0,
		n:                  v.N,
		prevMD:             prevProposalMetadata,
		decisionsPerLeader: v.DecisionsPerLeader,
		preparesFrom:       prepareAcknowledgements,
		f:                  f,
		logger:             v.Logger,
		metricsBlacklist:   v.MetricsBlacklist,
		nodes:              v.Comm.Nodes(),
		currView:           v.Number,
	}

	expectedBlacklist := blacklist.computeUpdate()
	if !equalIntLists(pendingBlacklist, expectedBlacklist) {
		return errors.Errorf("proposed blacklist %v differs from expected %v blacklist", pendingBlacklist, expectedBlacklist)
	}

	return nil
}

func (v *View) handlePrevSeqMessage(msgProposalSeq, sender uint64, m *protos.Message) {
	if m.GetPrePrepare() != nil {
		v.Logger.Warnf("Got pre-prepare for sequence %d but we're in sequence %d", msgProposalSeq, v.ProposalSequence)
		return
	}
	msgType := "prepare"
	if m.GetCommit() != nil {
		msgType = "commit"
	}

	var found bool

	switch msgType {
	case "prepare":
		// This is an assist message, we don't need to reply to it.
		if m.GetPrepare().Assist {
			return
		}
		if v.prevPrepareSent != nil {
			v.Comm.SendConsensus(sender, v.prevPrepareSent)
			found = true
		}
	case "commit":
		// This is an assist message, we don't need to reply to it.
		if m.GetCommit().Assist {
			return
		}
		if v.prevCommitSent != nil {
			v.Comm.SendConsensus(sender, v.prevCommitSent)
			found = true
		}
	}

	prevMsgFound := fmt.Sprintf("but didn't have a previous %s to send back.", msgType)
	if found {
		prevMsgFound = fmt.Sprintf("sent back previous %s.", msgType)
	}
	v.Logger.Debugf("Got %s for previous sequence (%d) from %d, %s", msgType, msgProposalSeq, sender, prevMsgFound)
}

func (v *View) discoverIfSyncNeeded(sender uint64, m *protos.Message) {
	// We're only interested in commit messages.
	commit := m.GetCommit()
	if commit == nil {
		return
	}

	// To commit a block we need 2f + 1 votes.
	// at least f+1 of them are honest and will broadcast
	// their commits to votes to everyone including us.
	// In each such a threshold of f+1 votes there is at least
	// a single honest node that prepared for a proposal
	// which we apparently missed.
	_, f := computeQuorum(v.N)
	threshold := f + 1

	v.lastVotedProposalByID[sender] = commit

	v.Logger.Debugf("Got commit of seq %d in view %d from %d while being in seq %d in view %d",
		commit.Seq, commit.View, sender, v.ProposalSequence, v.Number)

	// If we haven't reached a threshold of proposals yet, abort.
	if len(v.lastVotedProposalByID) < threshold {
		return
	}

	// Make a histogram out of all current seen votes.
	countsByVotes := make(map[proposalInfo]int)
	for _, vote := range v.lastVotedProposalByID {
		info := proposalInfo{
			digest: vote.Digest,
			view:   vote.View,
			seq:    vote.Seq,
		}
		countsByVotes[info]++
	}

	// Check if there is a <digest, view, seq> that collected a threshold of votes,
	// and that sequence is higher than our current sequence, or our view is different.
	for vote, count := range countsByVotes {
		if count < threshold {
			continue
		}

		// Disregard votes for past views.
		if vote.view < v.Number {
			continue
		}

		// Disregard votes for past sequences for this view.
		if vote.seq <= v.ProposalSequence && vote.view == v.Number {
			continue
		}

		v.Logger.Warnf("Seen %d votes for digest %s in view %d, sequence %d but I am in view %d and seq %d",
			count, vote.digest, vote.view, vote.seq, v.Number, v.ProposalSequence)
		v.stop()
		v.Sync.Sync()
		return
	}
}

type voteVerifier struct {
	v              *View
	proposal       *types.Proposal
	expectedDigest string
	validVotes     chan types.Signature
}

func (vv *voteVerifier) verifyVote(vote *protos.Message) {
	vv.v.Logger.Infof("Node %d verifyVote called", vv.v.SelfID)
	if vote == nil {
		vv.v.Logger.Warnf("Got nil vote in verifyVote")
		return
	}
	commit := vote.GetCommit()
	if commit == nil {
		vv.v.Logger.Warnf("Got nil commit in verifyVote")
		return
	}
	vv.v.Logger.Infof("Node %d verifying commit from %d, digest match: %v", vv.v.SelfID, commit.Signature.Signer, commit.Digest == vv.expectedDigest)
	if commit.Digest != vv.expectedDigest {
		vv.v.Logger.Warnf("Got wrong digest at processCommits for seq %d, expected %s, got %s", commit.Seq, vv.expectedDigest, commit.Digest)
		return
	}

	vv.v.Logger.Infof("Node %d calling VerifyConsenterSig for signer %d", vv.v.SelfID, commit.Signature.Signer)
	_, err := vv.v.Verifier.VerifyConsenterSig(types.Signature{
		ID:    commit.Signature.Signer,
		Value: commit.Signature.Value,
		Msg:   commit.Signature.Msg,
	}, *vv.proposal)
	if err != nil {
		vv.v.Logger.Warnf("Node %d couldn't verify %d's signature: %v", vv.v.SelfID, commit.Signature.Signer, err)
		return
	}

	vv.v.Logger.Infof("Node %d signature verification successful for %d, adding to validVotes", vv.v.SelfID, commit.Signature.Signer)
	vv.validVotes <- types.Signature{
		ID:    commit.Signature.Signer,
		Value: commit.Signature.Value,
		Msg:   commit.Signature.Msg,
	}
}

func (v *View) decide(proposal *types.Proposal, signatures []types.Signature, requests []types.RequestInfo) {
	v.Logger.Infof("Deciding on seq %d", v.ProposalSequence)
	v.ViewSequences.Store(ViewSequence{ProposalSeq: v.ProposalSequence, ViewActive: true})
	// first make preparations for the next sequence so that the view will be ready to continue right after delivery
	v.startNextSeq()
	signatures = append(signatures, *v.myProposalSig)
	v.Decider.Decide(*proposal, signatures, requests)
}

func (v *View) startNextSeq() {
	prevSeq := v.ProposalSequence

	v.ProposalSequence++
	v.DecisionsInView++

	nextSeq := v.ProposalSequence

	v.MetricsView.ProposalSequence.Set(float64(v.ProposalSequence))
	v.MetricsView.DecisionsInView.Set(float64(v.DecisionsInView))

	// Update role as DecisionsInView has changed
	v.updateMyRole()

	// Reset hierarchical consensus flags for new sequence
	v.sentPrepareForSequence = false
	v.sentCommitForSequence = false

	v.Logger.Infof("Sequence: %d-->%d", prevSeq, nextSeq)

	// swap next prePrepare
	tmp := v.prePrepare
	v.prePrepare = v.nextPrePrepare
	// clear tmp
	for len(tmp) > 0 {
		<-tmp
	}
	tmp = make(chan *protos.Message, 1)
	v.nextPrePrepare = tmp

	// swap next prepares
	tmpVotes := v.prepares
	v.prepares = v.nextPrepares
	tmpVotes.clear(v.N)
	v.nextPrepares = tmpVotes

	// swap next commits
	tmpVotes = v.commits
	v.commits = v.nextCommits
	tmpVotes.clear(v.N)
	v.nextCommits = tmpVotes
}

// GetMetadata returns the current sequence and view number (in a marshaled ViewMetadata protobuf message)
func (v *View) GetMetadata() []byte {
	metadata := &protos.ViewMetadata{
		ViewId:          v.Number,
		LatestSequence:  v.ProposalSequence,
		DecisionsInView: v.DecisionsInView,
	}

	v.Logger.Debugf("GetMetadata with view %d, seq %d, dec %d", metadata.ViewId, metadata.LatestSequence, metadata.DecisionsInView)

	var (
		prevSigs []*protos.Signature
		prevProp *protos.Proposal
	)
	verificationSeq := v.Verifier.VerificationSequence()

	prevProp, prevSigs = v.RetrieveCheckpoint()

	prevMD := &protos.ViewMetadata{}
	if err := proto.Unmarshal(prevProp.Metadata, prevMD); err != nil {
		v.Logger.Panicf("Attempted to propose a proposal with invalid unchanged previous proposal view metadata: %v", err)
	}

	metadata.BlackList = prevMD.BlackList

	metadata = v.metadataWithUpdatedBlacklist(metadata, verificationSeq, prevProp, prevSigs)
	metadata = v.bindCommitSignaturesToProposalMetadata(metadata, prevSigs)

	return MarshalOrPanic(metadata)
}

func (v *View) metadataWithUpdatedBlacklist(metadata *protos.ViewMetadata, verificationSeq uint64, prevProp *protos.Proposal, prevSigs []*protos.Signature) *protos.ViewMetadata {
	var membershipChange bool
	if v.MembershipNotifier != nil {
		membershipChange = v.MembershipNotifier.MembershipChange()
	}

	if verificationSeq == prevProp.VerificationSequence && !membershipChange {
		v.Logger.Debugf("Proposing proposal %d with verification sequence of %d and %d commit signatures",
			v.ProposalSequence, verificationSeq, len(prevSigs))
		return v.updateBlacklistMetadata(metadata, prevSigs, prevProp.Metadata)
	}

	if verificationSeq != prevProp.VerificationSequence {
		v.Logger.Infof("Skipping updating blacklist due to verification sequence changing from %d to %d",
			prevProp.VerificationSequence, verificationSeq)
	}
	if membershipChange {
		v.Logger.Infof("Skipping updating blacklist due to membership change")
	}

	return metadata
}

// Propose broadcasts a prePrepare message with the given proposal
func (v *View) Propose(proposal types.Proposal) {
	_, prevSigs := v.RetrieveCheckpoint()

	seq := v.ProposalSequence
	msg := &protos.Message{
		Content: &protos.Message_PrePrepare{
			PrePrepare: &protos.PrePrepare{
				View: v.Number,
				Seq:  seq,
				Proposal: &protos.Proposal{
					Header:               proposal.Header,
					Payload:              proposal.Payload,
					Metadata:             proposal.Metadata,
					VerificationSequence: uint64(proposal.VerificationSequence),
				},
				PrevCommitSignatures: prevSigs,
			},
		},
	}

	// Send the proposal to yourself first
	v.HandleMessage(v.LeaderID, msg)

	// Get nodes list and check if hierarchical consensus is enabled
	nodesList := v.Comm.Nodes()

	// Hierarchical consensus: Primary sends only to Secondaries
	hierarchicalInfo := GetHierarchicalInfo(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
	if hierarchicalInfo.Role == PRIMARY {
		v.Logger.Infof("PRIMARY %d sending PrePrepare to secondaries %v", v.SelfID, hierarchicalInfo.Secondaries)
		for _, secondary := range hierarchicalInfo.Secondaries {
			v.Comm.SendConsensus(secondary, msg)
		}
		// Send hierarchical heartbeat after sending PrePrepare
		v.sendHierarchicalHeartbeat()
	} else {
		v.Logger.Warnf("Non-primary node %d attempted to propose", v.SelfID)
	}

	v.Logger.Debugf("Proposing proposal sequence %d in view %d", seq, v.Number)
}

func (v *View) bindCommitSignaturesToProposalMetadata(metadata *protos.ViewMetadata, prevSigs []*protos.Signature) *protos.ViewMetadata {
	if v.DecisionsPerLeader == 0 {
		v.Logger.Debugf("Leader rotation is disabled, will not bind signatures to proposals")
		return metadata
	}
	metadata.PrevCommitSignatureDigest = CommitSignaturesDigest(prevSigs)

	if len(metadata.PrevCommitSignatureDigest) == 0 {
		v.Logger.Debugf("No previous commit signatures detected")
	} else {
		v.Logger.Debugf("Bound %d commit signatures to proposal", len(prevSigs))
	}
	return metadata
}

func (v *View) stop() {
	v.stopOnce.Do(func() {
		if v.abortChan == nil {
			return
		}
		close(v.abortChan)
	})
}

// Abort forces the view to end
func (v *View) Abort() {
	v.stop()
	v.viewEnded.Wait()
}

func (v *View) Stopped() bool {
	select {
	case <-v.abortChan:
		return true
	default:
		return false
	}
}

func (v *View) GetLeaderID() uint64 {
	return v.LeaderID
}

func (v *View) updateBlacklistMetadata(metadata *protos.ViewMetadata, prevSigs []*protos.Signature, prevMetadata []byte) *protos.ViewMetadata {
	if v.DecisionsPerLeader == 0 {
		v.Logger.Debugf("Rotation is disabled, setting blacklist to be empty")
		metadata.BlackList = nil
		return metadata
	}

	preparesFrom := make(map[uint64]*protos.PreparesFrom)

	for _, sig := range prevSigs {
		aux := v.Verifier.AuxiliaryData(sig.Msg)
		prpf := &protos.PreparesFrom{}
		if err := proto.Unmarshal(aux, prpf); err != nil {
			v.Logger.Panicf("Failed unmarshalling auxiliary data from previously persisted signatures: %v", err)
		}
		preparesFrom[sig.Signer] = prpf
	}

	prevMD := &protos.ViewMetadata{}
	if err := proto.Unmarshal(prevMetadata, prevMD); err != nil {
		v.Logger.Panicf("Attempted to propose a proposal with invalid previous proposal view metadata: %v", err)
	}

	_, f := computeQuorum(v.N)

	blacklist := &blacklist{
		currentLeader:      v.LeaderID,
		leaderRotation:     v.DecisionsPerLeader > 0,
		currView:           metadata.ViewId,
		prevMD:             prevMD,
		nodes:              v.Comm.Nodes(),
		f:                  f,
		n:                  v.N,
		logger:             v.Logger,
		metricsBlacklist:   v.MetricsBlacklist,
		preparesFrom:       preparesFrom,
		decisionsPerLeader: v.DecisionsPerLeader,
	}
	metadata.BlackList = blacklist.computeUpdate()
	return metadata
}

func (v *View) blacklistingSupported(f int, myLastCommitSignatures []*protos.Signature) bool {
	// Once we blacklist, there is no way back. This is a one way trip, unless we downgrade the version
	// in all nodes and view change.
	if v.blacklistSupported {
		return true
	}
	// We wish to find whether there are f+1 witnesses for blacklisting being
	// activated among the signed commits of the previous proposal.
	var count int
	for _, commitSig := range myLastCommitSignatures {
		aux := v.Verifier.AuxiliaryData(commitSig.Msg)
		if len(aux) > 0 {
			count++
		}
	}

	v.Logger.Debugf("Found %d out of %d required witnesses for auxiliary data", count, f+1)

	blacklistSupported := count > f

	// We cache the result in case it is 'true'.
	// Subsequent invocations will skip the parsing.
	v.blacklistSupported = v.blacklistSupported || blacklistSupported
	return blacklistSupported
}
func (v *View) getRequiredPreparesToProceed() int {
	// Get current role dynamically
	nodesList := v.Comm.Nodes()
	currentRole := GetNodeRole(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)

	// Hierarchical consensus logic
	switch currentRole {
	case PRIMARY:
		// Primary needs prepares from all secondaries plus its own
		secondaries := GetSecondaryNodes(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		return len(secondaries) + 1 // +1 for primary's own vote
	case SECONDARY:
		// Secondary processes its own prepare phase immediately after sending prepares to others
		// It doesn't wait for prepares from other secondaries in the prepare phase
		return 0
	case FOLLOWER:
		// Follower doesn't collect prepares, it sends them
		return 0
	default:
		// This should never happen in hierarchical consensus
		v.Logger.Panicf("Unknown role %v in hierarchical consensus", currentRole)
		return 0
	}
}

func (v *View) getPreparesChannel() <-chan *vote {
	switch v.MyRole {
	case PRIMARY:
		// Primary uses prepares channel which contains both its own vote and secondary votes
		if v.prepares != nil && v.prepares.votes != nil {
			return v.prepares.votes
		}
	case SECONDARY:
		if v.followerVotes != nil && v.followerVotes.votes != nil {
			return v.followerVotes.votes
		}
		if v.prepares != nil && v.prepares.votes != nil {
			return v.prepares.votes
		}
	default:
		if v.prepares != nil && v.prepares.votes != nil {
			return v.prepares.votes
		}
	}

	// Return a closed channel to avoid blocking if nothing is available
	closedChan := make(chan *vote)
	close(closedChan)
	return closedChan
}

func (v *View) getRequiredCommitsToProceed() int {
	// Get current role dynamically
	nodesList := v.Comm.Nodes()
	currentRole := GetNodeRole(v.SelfID, v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)

	// Hierarchical consensus logic
	switch currentRole {
	case PRIMARY:
		// Primary needs commits from all secondaries plus its own
		secondaries := GetSecondaryNodes(v.Number, v.N, nodesList, v.DecisionsInView, v.DecisionsPerLeader)
		return len(secondaries) + 1 // +1 for primary's own vote
	case SECONDARY:
		// Secondary processes its own commit phase immediately after sending commits to others
		// It doesn't wait for commits from other secondaries in the commit phase
		return 0
	case FOLLOWER:
		// Follower doesn't collect commits, it sends them
		return 0
	default:
		// This should never happen in hierarchical consensus
		v.Logger.Panicf("Unknown role %v in hierarchical consensus", currentRole)
		return 0
	}
}

func (v *View) getCommitsChannel() <-chan *vote {
	switch v.MyRole {
	case PRIMARY:
		// Primary uses commits channel which contains both its own vote and secondary votes
		if v.commits != nil && v.commits.votes != nil {
			return v.commits.votes
		}
	case SECONDARY:
		if v.followerVotes != nil && v.followerVotes.votes != nil {
			return v.followerVotes.votes
		}
		if v.commits != nil && v.commits.votes != nil {
			return v.commits.votes
		}
	default:
		if v.commits != nil && v.commits.votes != nil {
			return v.commits.votes
		}
	}

	// Return a closed channel to avoid blocking if nothing is available
	closedChan := make(chan *vote)
	close(closedChan)
	return closedChan
}
