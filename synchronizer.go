package main

import (
	bft "github.com/hyperledger/binibft-poc/consensus/pkg/types"
)

func (*Node) Sync() bft.SyncResponse {
	panic("implement me")
}
