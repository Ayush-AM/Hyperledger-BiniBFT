package main

import bft "github.com/hyperledger/binibft-poc/consensus/pkg/types"

func (*Node) RequestID(req []byte) bft.RequestInfo {
	txn := TransactionFromBytes(req)
	return bft.RequestInfo{
		ClientID: txn.ClientID,
		ID:       txn.ID,
	}
}
