package main

import (
	bft "github.com/hyperledger/binibft-poc/consensus/pkg/types"
	"github.com/hyperledger/binibft-poc/consensus/binibftprotos"
	"fmt"
	"strconv"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// store blocks in LevelDB
func (n *Node) Deliver(proposal bft.Proposal, signature []bft.Signature) bft.Reconfig {
	blockData := BlockDataFromBytes(proposal.Payload)
	metadata := &binibftprotos.ViewMetadata{}
	if err := proto.Unmarshal(proposal.Metadata, metadata); err != nil {
		panic(fmt.Sprintf("Unable to unmarshal metadata, error: %v", err))
	}
	log.Infof("Node %d received proposal with sequence %v", n.id, metadata)
	txns := make([]Transaction, 0, len(blockData.Transactions))
	for _, rawTxn := range blockData.Transactions {
		txn := TransactionFromBytes(rawTxn)
		txns = append(txns, Transaction{
			ClientID: txn.ClientID,
			TS:       txn.TS,
			ID:       txn.ID,
			Data:     txn.Data,
		})
	}
	header := BlockHeaderFromBytes(proposal.Header)

	block := &Block{
		Sequence:     header.Sequence,
		PrevHash:     header.PrevHash,
		Transactions: txns,
		Metadata:     proposal.Metadata,
	}

	n.deliverChan <- block
	// store block
	err := n.db.Put([]byte(strconv.FormatUint(uint64(header.Sequence), 10)), block.ToBytes(), &opt.WriteOptions{})
	if err != nil {
		log.Panicf("Error storing block: %v", err)
	}
	// set index for latest block
	err = n.db.Put([]byte("__latest_block"), block.ToBytes(), &opt.WriteOptions{})
	if err != nil {
		log.Panicf("Error storing latest block: %v", err)
	}

	// Update prevHash for the next block
	n.prevHash = computeDigest(block.ToBytes())
	log.Infof("Node %d stored block with sequence %v, updated prevHash: %s", n.id, header.Sequence, n.prevHash)

	return bft.Reconfig{
		InLatestDecision: false, // has the configuration changed?
	}
}
