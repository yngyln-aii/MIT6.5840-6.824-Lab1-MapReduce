package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		response := doHeartbeat()
		log.Printf("Worker: receive coordinator's response, new job is %v \n", response)

		switch response.JobType {
		case MapJob:
			doMapTask(mapf, response)
		case ReduceJob:
			doReduceTask(reducef, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("worker get an unexpected jobType %v", response.JobType))
		}
	}

}

func doMapTask(mapF func(string, string) []KeyValue, response *HeartbeatResponse) {
	wordCountList := getWordCountListOfFile(mapF, response.FilePath)

	intermediate := splitWordCountListToReduceNNum(wordCountList, response.NReduce)

	var writeIntermediateFilewg sync.WaitGroup
	for reduceNumber, splitedWordCountList := range intermediate {
		writeIntermediateFilewg.Add(1)
		go func(reduceNumber int, splitedWordCountList []KeyValue) {
			defer writeIntermediateFilewg.Done()
			writeIntermediateFile(response.Id, reduceNumber, splitedWordCountList)
		}(reduceNumber, splitedWordCountList)
	}
	writeIntermediateFilewg.Wait()

	doReport(response.Id, MapPhase)
}

func getWordCountListOfFile(mapF func(string, string) []KeyValue, filePath string) []KeyValue {
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filePath)
	}
	return mapF(filePath, string(content))
}

func splitWordCountListToReduceNNum(wordCountList []KeyValue, nReduce int) [][]KeyValue {
	intermediate := make([][]KeyValue, nReduce)
	for _, wordCount := range wordCountList {
		word := wordCount.Key
		reduceNumber := ihash(word) % nReduce
		intermediate[reduceNumber] = append(intermediate[reduceNumber], wordCount)
	}
	return intermediate
}

func writeIntermediateFile(mapNumber int, reduceNumber int, wordCountList []KeyValue) {
	fileName := generateMapResultFileName(mapNumber, reduceNumber)
	file, err := os.Create(fileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot create %v", fileName)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, wordCount := range wordCountList {
		err := enc.Encode(&wordCount)
		if err != nil {
			log.Fatalf("cannot encode %v", wordCount)
		}
	}
	atomicWriteFile(fileName, &buf)
}

func doReduceTask(reduceF func(string, []string) string, response *HeartbeatResponse) {
	wordCountList := getWordCountListFromIntermediateFile(response.NMap, response.Id)

	wordCountMap := gatherAndSortIntermediateWordCountList(wordCountList)

	var buf bytes.Buffer
	reducIntermediateWordCount(reduceF, wordCountMap, &buf)

	fileName := generateReduceResultFileName(response.Id)
	atomicWriteFile(fileName, &buf)

	doReport(response.Id, ReducePhase)
}

func getWordCountListFromIntermediateFile(NMap int, reduceNumer int) []KeyValue {
	var wordCountList []KeyValue
	for mapNumber := 0; mapNumber < NMap; mapNumber++ {
		splitedWordCountList := decodeWordCountListFromIntermediateFile(mapNumber, reduceNumer)
		wordCountList = append(wordCountList, splitedWordCountList...)
	}

	return wordCountList
}

func gatherAndSortIntermediateWordCountList(wordCountList []KeyValue) map[string][]string {
	wordCountMap := make(map[string][]string)
	for _, wordCount := range wordCountList {
		word := wordCount.Key
		count := wordCount.Value
		wordCountMap[word] = append(wordCountMap[word], count)
	}

	return wordCountMap
}

func reducIntermediateWordCount(reduceF func(string, []string) string, wordCountMap map[string][]string, buf *bytes.Buffer) {
	for word, counts := range wordCountMap {
		reduceResult := reduceF(word, counts)
		fmt.Fprintf(buf, "%v %v\n", word, reduceResult)
	}
}

func decodeWordCountListFromIntermediateFile(mapNumber int, reduceNumer int) []KeyValue {
	filePath := generateMapResultFileName(mapNumber, reduceNumer)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	defer file.Close()

	var wordCountList []KeyValue
	dec := json.NewDecoder(file)
	for {
		var wordCount KeyValue
		if err := dec.Decode(&wordCount); err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("cannot decode %v", filePath)
		}
		wordCountList = append(wordCountList, wordCount)
	}

	return wordCountList
}

func atomicWriteFile(filename string, reader io.Reader) (err error) {
	tmpFileName, err := writeToTmpFile(filename, reader)
	if err != nil {
		return fmt.Errorf("cannot write to temp file: %v", err)
	}

	if err := os.Rename(tmpFileName, filename); err != nil {
		return fmt.Errorf("cannot rename temp file: %v", err)
	}

	return nil
}

func writeToTmpFile(filename string, reader io.Reader) (tmpFileName string, err error) {
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}

	tmpFile, err := os.CreateTemp(dir, file)
	if err != nil {
		return "", fmt.Errorf("cannot create temp file: %v", err)
	}
	defer tmpFile.Close()
	defer func() {
		if err != nil {
			os.Remove(tmpFile.Name())
		}
	}()

	_, err = io.Copy(tmpFile, reader)
	if err != nil {
		return "", fmt.Errorf("cannot write to temp file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("cannot close temp file: %v", err)
	}

	return tmpFile.Name(), nil
}

func doHeartbeat() *HeartbeatResponse {
	response := HeartbeatResponse{}
	isSuccess := call("Coordinator.Heartbeat", &HeartbeatRequest{}, &response)
	if !isSuccess {
		log.Printf("Worker: call Coordinator.Heartbeat failed, worker will exit.")
		response.JobType = CompleteJob
	}
	return &response
}

func doReport(id int, phase SchedulePhase) {
	call("Coordinator.Report", &ReportRequest{Id: id, Phase: phase}, &ReportResponse{})
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
