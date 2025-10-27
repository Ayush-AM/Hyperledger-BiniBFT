package main

import bft "github.com/hyperledger/binibft-poc/consensus/pkg/types"

func (*Node) Sign(msg []byte) []byte {
	return nil
}

func (n *Node) SignProposal(bft.Proposal, []byte) *bft.Signature {
	return &bft.Signature{
		ID: n.id,
	}
}
