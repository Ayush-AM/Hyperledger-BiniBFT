package main

import (
	bft "github.com/hyperledger/binibft-poc/consensus/pkg/types"
	"github.com/hyperledger/binibft-poc/consensus/binibftprotos"
	"fmt"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

func (n *Node) AssembleProposal(metadata []byte, requests [][]byte) bft.Proposal {
	log.Infof("Node %d assembling proposal with %d requests", n.id, len(requests))
	blockData := BlockData{Transactions: requests}.ToBytes()
	md := &binibftprotos.ViewMetadata{}
	if err := proto.Unmarshal(metadata, md); err != nil {
		panic(fmt.Sprintf("Unable to unmarshal metadata, error: %v", err))
	}
	return bft.Proposal{
		Header: BlockHeader{
			PrevHash: n.prevHash,
			DataHash: computeDigest(blockData),
			Sequence: int64(md.LatestSequence),
		}.ToBytes(),
		Payload:  BlockData{Transactions: requests}.ToBytes(),
		Metadata: metadata,
	}
}
