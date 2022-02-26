package surfstore

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
)

const (
	Unchanged int = 0
	Modified  int = 1
	New       int = 2
	Deleted   int = 3
)

type FileInfo struct {
	FileMetaData FileMetaData
	Status       int
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// Read base
	baseDir, dirErr := ioutil.ReadDir(client.BaseDir)
	if dirErr != nil {
		log.Println("Error occurs when reading base Dir of client : ", dirErr)
	}
	dirMap := make(map[string]os.FileInfo)
	for _, f := range baseDir {
		dirMap[f.Name()] = f
	}

	// Check index.txt
	idxFilePath := client.BaseDir + "/index.txt"
	if _, idxFileErr := os.Stat(idxFilePath); os.IsNotExist(idxFileErr) {
		file, _ := os.Create(idxFilePath)
		defer file.Close()
	}

	idxMap := make(map[string]int)

	idxFile, _ := ioutil.ReadFile(idxFilePath)
	idxLines := strings.Split(string(idxFile), "\n")
	for i, line := range idxLines {
		if line != "" {
			fileMetaData := NewFileMetaDataFromConfig(string(line))
			idxMap[fileMetaData.Filename] = i
		}
	}
	fileMetaMap, errLoadMeta := LoadMetaFromMetaFile(client.BaseDir)
	if errLoadMeta != nil {
		log.Println("Error occurs when LoadMetaFromMetaFile : ", errLoadMeta)
	}
	PrintMetaMap(fileMetaMap)

	// Iterate baseDir files and sync with index.txt, update file status in a new map
	baseFileInfoMap := baseSync(client, &idxMap, &idxLines, fileMetaMap, dirMap)

	// var succ bool
	serverFileInfoMap := make(map[string]*FileMetaData)
	getFileInfoMapErr := client.GetFileInfoMap(&serverFileInfoMap)
	if getFileInfoMapErr != nil {
		log.Println("Error when getting FileInfoMap from server: ", getFileInfoMapErr)
	}

	// loop through local base
	for fileName, info := range baseFileInfoMap {
		// The file is in the server
		if _, ok := serverFileInfoMap[fileName]; ok {
			serverFileMetaData := serverFileInfoMap[fileName]
			clientFileMetaData := &info.FileMetaData
			if clientFileMetaData.Version == serverFileMetaData.Version && info.Status == Unchanged {
				continue
			} else if (clientFileMetaData.Version == serverFileMetaData.Version && info.Status == Modified) ||
				(clientFileMetaData.Version > serverFileMetaData.Version) {
				putFileToServer(client, clientFileMetaData, idxMap, &idxLines, info.Status)
			} else {
				getFileFromServer(client, serverFileMetaData, idxMap, &idxLines)
			}
		} else { // The file is not in the server
			uploadToServer(client, &info.FileMetaData, idxMap, &idxLines)
		}
	}

	// Sync with the server
	for fileName, serverFileMetaData := range serverFileInfoMap {
		if _, ok1 := baseFileInfoMap[fileName]; !ok1 {
			if _, ok2 := idxMap[fileName]; ok2 { // The file has been deleted
				deletedFileMetaData := NewFileMetaDataFromConfig(idxLines[idxMap[fileName]])
				if deletedFileMetaData.Version <= serverFileMetaData.Version {
					getFileFromServer(client, serverFileMetaData, idxMap, &idxLines)
				} else {
					putFileToServer(client, deletedFileMetaData, idxMap, &idxLines, Deleted)
				}
			} else {
				// The file is a new file
				line, downloadErr := getFile(client, fileName, serverFileMetaData)
				if downloadErr != nil {
					log.Println("Error when download new file: ", downloadErr)
				}
				idxLines = append((idxLines), line)
			}
		}
	}

	// Update index.txt file
	newIndexFile := ""
	for _, indexLine := range idxLines {
		if indexLine == "" {
			continue
		}
		newIndexFile += indexLine + "\n"
	}

	writeIdxErr := ioutil.WriteFile(idxFilePath, []byte(newIndexFile), 0755)
	if writeIdxErr != nil {
		log.Println("Error when updating index.txt: ", writeIdxErr)
	}
}

/*
Helper function below
*/

// Set the hashList to "0" if the file is deleted
func deletedCheck(idxFileInfoMap map[string]*FileMetaData, idxMap map[string]int, dirMap map[string]os.FileInfo, idxLines *[]string) {
	for file, filemetadata := range idxFileInfoMap {
		if _, ok := dirMap[file]; !ok {
			// The file in the index.txt has been deleted, set hashlist to "0" and increase version by 1
			idx := idxMap[file]
			if len(filemetadata.BlockHashList) == 1 && filemetadata.BlockHashList[0] == "0" {
				(*idxLines)[idx] = filemetadata.Filename + "," + strconv.Itoa(int(filemetadata.Version)) + ",0"
			} else {
				(*idxLines)[idx] = filemetadata.Filename + "," + strconv.Itoa(int(filemetadata.Version+1)) + ",0"
			}
		}
	}
}

// 1. Sync local dir using index.txt
// 2. Deleted file -> set the hashlist to "0"
// 3. Modified file -> update fileInfo, after comparing with server files, then updating
// &idxMap, &idxLines, fileMetaMap, dirMap
func baseSync(client RPCClient, idxMap *map[string]int, idxLines *[]string, idxFileInfoMap map[string]*FileMetaData, dirMap map[string]os.FileInfo) map[string]*FileInfo {
	// Check if any file is deleted
	deletedCheck(idxFileInfoMap, *idxMap, dirMap, idxLines)

	baseMap := make(map[string]*FileInfo)

	// Deal with each file except index.txt
	for fileDir, f := range dirMap {
		if fileDir == "index.txt" {
			continue
		}

		file, openErr := os.Open(ConcatPath(client.BaseDir, fileDir))
		if openErr != nil {
			log.Println("Error when open file in baseSync: ", openErr)
		}
		fileSize := f.Size()
		numBlock := int(math.Ceil(float64(fileSize) / float64(client.BlockSize)))

		// Record status in FileInfo
		var info FileInfo

		// The file is in the index.txt
		if fileMetaData, ok := idxFileInfoMap[fileDir]; ok {
			changed, hashList := generateHashList(file, fileMetaData, numBlock, client.BlockSize)
			info.FileMetaData.Filename = fileDir
			info.FileMetaData.Version = fileMetaData.Version
			hashStr := ""
			for i, h := range hashList {
				info.FileMetaData.BlockHashList = append(info.FileMetaData.BlockHashList, h)
				hashStr += h
				if i != len(hashList)-1 {
					hashStr += " "
				}
			}
			if changed {
				info.Status = Modified
				idx := (*idxMap)[fileDir]
				(*idxLines)[idx] = fileDir + "," + strconv.Itoa(int(fileMetaData.Version)+1) + "," + hashStr // version ?
			} else {
				info.Status = Unchanged
			}
		} else { // File not in index.txt
			var metaData FileMetaData
			_, hashList := generateHashList(file, &metaData, numBlock, client.BlockSize)
			info.FileMetaData.Filename = fileDir
			info.FileMetaData.Version = 1
			hashStr := ""

			for idx, h := range hashList {
				info.FileMetaData.BlockHashList = append(info.FileMetaData.BlockHashList, h)
				hashStr += h
				if idx != len(hashList)-1 {
					hashStr += " "
				}
			}
			fmt.Printf("%d\n", len(info.FileMetaData.BlockHashList))
			info.Status = New
			// info.FileMetaData.BlockHashList = hashList
			*idxLines = append((*idxLines), fileDir+","+strconv.Itoa(int(info.FileMetaData.Version))+","+hashStr)

			// Update idxMap
			(*idxMap)[fileDir] = len(*idxLines) - 1
		}
		baseMap[fileDir] = &info
		file.Close()
	}
	// PrintMetaMap(baseMap)
	return baseMap
}

//Generate hashList from file data blocks.
func generateHashList(file *os.File, fileMetaData *FileMetaData, numBlock int, blockSize int) (bool, []string) {
	hashList := make([]string, numBlock)
	var changed bool
	changed = false

	for i := 0; i < numBlock; i++ {
		buf := make([]byte, blockSize)
		n, err := file.Read(buf)
		if err != nil {
			log.Println("HashList read error: ", err)
		}

		buf = buf[:n]
		hashString := GetBlockHashString(buf)
		hashList[i] = hashString
		if i >= len(fileMetaData.BlockHashList) {
			fmt.Print("Length longer\n")
			changed = true
		} else if hashString != fileMetaData.BlockHashList[i] {
			fmt.Printf("Block %d diff\n", i)
			changed = true
		}
	}
	if numBlock != len(fileMetaData.BlockHashList) {
		fmt.Printf("numBlock = %d\n", numBlock)
		fmt.Printf("len(fileMetaData.BlockHashList) = %d\n", len(fileMetaData.BlockHashList))
		fmt.Print("numBlock != len(fileMetaData.BlockHashList)\n")
		changed = true
	}
	return changed, hashList
}

// Upload files
func uploadToServer(client RPCClient, fileMetaData *FileMetaData, idxMap map[string]int, idxLines *[]string) error {
	var blockStoreAddr string
	client.GetBlockStoreAddr(&blockStoreAddr)

	filePath := ConcatPath(client.BaseDir, fileMetaData.Filename)
	// file deleted
	if _, e := os.Stat(filePath); os.IsNotExist(e) {
		updateErr := client.UpdateFile(fileMetaData, &fileMetaData.Version)
		if updateErr != nil {
			log.Println("Error when updating file : ", updateErr)
		}
		return updateErr
	}

	f, openErr := os.Open(filePath)
	if openErr != nil {
		log.Println("Error when opening the file: ", openErr)
	}

	defer f.Close()

	file, _ := os.Stat(filePath)
	numBlock := int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))

	for i := 0; i < numBlock; i++ {
		var block Block
		block.BlockData = make([]byte, client.BlockSize)
		n, readFileErr := f.Read(block.BlockData)
		if readFileErr != nil && readFileErr != io.EOF {
			log.Println("Error when reading the file: ", readFileErr)
		}
		block.BlockSize = int32(n)
		block.BlockData = block.BlockData[:n]

		var succ bool
		putBlockErr := client.PutBlock(&block, blockStoreAddr, &succ)
		if putBlockErr != nil {
			log.Println("Error when putting block: ", putBlockErr)
		}
	}

	updateFileErr := client.UpdateFile(fileMetaData, &fileMetaData.Version)
	if updateFileErr != nil {
		log.Println("Error when using client.UpdateFile: ", updateFileErr)
		// sync with the server if the update fails
		serverFileInfoMap := make(map[string]*FileMetaData)
		client.GetFileInfoMap(&serverFileInfoMap)
		getFileFromServer(client, serverFileInfoMap[fileMetaData.Filename], idxMap, idxLines)
	}
	return updateFileErr
}

func putFileToServer(client RPCClient, clientFileMetaData *FileMetaData, idxMap map[string]int, idxLines *[]string, s int) {
	if s == Modified {
		clientFileMetaData.Version += 1
		idx := idxMap[clientFileMetaData.Filename]
		line := (*idxLines)[idx]
		(*idxLines)[idx] = line[:strings.Index(line, ",")] + "," + strconv.Itoa(int(clientFileMetaData.Version)) + "," + line[strings.LastIndex(line, ",")+1:]
	}

	err := uploadToServer(client, clientFileMetaData, idxMap, idxLines)
	if err != nil {
		log.Println("Error when uploading file: ", err)
	}
}

func getFileFromServer(client RPCClient, serverFileMetaData *FileMetaData, idxMap map[string]int, idxLines *[]string) {
	line, err := getFile(client, serverFileMetaData.Filename, serverFileMetaData)
	if err != nil {
		log.Println("Error when downloading file: ", err)
	}

	idx := idxMap[serverFileMetaData.Filename]
	(*idxLines)[idx] = line
}

func getFile(client RPCClient, fileDir string, fileMetaData *FileMetaData) (string, error) {
	var blockStoreAddr string
	client.GetBlockStoreAddr(&blockStoreAddr)

	filePath := ConcatPath(client.BaseDir, fileDir)

	if fileMetaData.BlockHashList[0] == "0" && len(fileMetaData.BlockHashList) == 1 {
		fileExist, errExist := exists(filePath)
		line := fileMetaData.Filename + "," + strconv.Itoa(int(fileMetaData.Version)) + ",0"
		if fileExist {
			removeFileErr := os.Remove(filePath)
			if removeFileErr != nil {
				log.Println("Cannot remove file: ", removeFileErr)
			}
			return line, removeFileErr
		} else if errExist != nil {
			log.Println("File path error when removing: ", errExist)
			return line, errExist
		} else {
			return line, errExist
		}
	}

	if _, e := os.Stat(filePath); os.IsNotExist(e) {
		os.Create(filePath)
	} else {
		os.Truncate(filePath, 0)
	}
	file, _ := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)

	hashStr := ""
	var err error
	for i, hash := range fileMetaData.BlockHashList {
		var blockData Block
		getBlockErr := client.GetBlock(hash, blockStoreAddr, &blockData)
		if getBlockErr != nil {
			err = getBlockErr
			log.Println("Error when getting block: ", getBlockErr)
		}

		data := string(blockData.BlockData)
		_, writeFileErr := io.WriteString(file, data)
		if writeFileErr != nil {
			err = writeFileErr
			log.Println("Error when writing file: ", writeFileErr)
		}

		hashStr += hash
		if i != len(fileMetaData.BlockHashList)-1 {
			hashStr += " "
		}
	}
	line := fileMetaData.Filename + "," + strconv.Itoa(int(fileMetaData.Version)) + "," + hashStr

	file.Close()
	return line, err
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
