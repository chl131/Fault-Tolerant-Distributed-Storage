package surfstore

import (
	context "context"
	"encoding/hex"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	getStr := blockHash.GetHash()
	if val, ok := bs.BlockMap[getStr]; ok {
		return val, nil
	} else {
		return nil, fmt.Errorf("Block does not exit")
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	s := Success{Flag: true}
	blockBytes := GetBlockHashBytes(block.BlockData)
	hashString := hex.EncodeToString(blockBytes)
	bs.BlockMap[hashString] = block
	return &s, nil // When will the error != nil ?
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	blockHashesOut := BlockHashes{Hashes: make([]string, 0)}
	for _, hashString := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hashString]; ok {
			blockHashesOut.Hashes = append(blockHashesOut.Hashes, hashString)
		}
	}
	return &blockHashesOut, nil // When will the error != nil ?
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
