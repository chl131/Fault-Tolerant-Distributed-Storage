package surfstore

import (
	context "context"
	"errors"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	mapRetrun := FileInfoMap{FileInfoMap: m.FileMetaMap}
	return &mapRetrun, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	filename := fileMetaData.Filename
	v := Version{Version: fileMetaData.Version}
	if _, ok := m.FileMetaMap[filename]; ok {
		if m.FileMetaMap[filename].Version == fileMetaData.Version-1 {
			m.FileMetaMap[filename] = fileMetaData
			return &v, nil
		} else {
			return &v, errors.New("Version error")
		}
	} else {
		m.FileMetaMap[filename] = fileMetaData
		return &v, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	blockStoreAddrReturn := BlockStoreAddr{Addr: m.BlockStoreAddr}
	return &blockStoreAddrReturn, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
