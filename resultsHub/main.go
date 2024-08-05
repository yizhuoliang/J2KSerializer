package main

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pb "github.com/yizhuoliang/J2KResultsHub"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ResultsHubServer struct {
	pb.UnimplementedResultsHubServer

	claimCellFinishedChan    chan *pb.VarResults
	claimAcknowledgementChan chan bool
	fetchVarRequestChan      chan *pb.FetchVarResultRequest
	fetchVarReplyChan        chan *pb.VarResult
	waitRequestChan          chan *pb.WaitCellRequest
	waitAcknowledgementChan  chan bool
	submitRecursiveVarsChan  chan *pb.VarResults
	replyRecursiveVarsChan   chan bool
}

// CyclicBuffer is a simple cyclic buffer.
type CyclicBuffer struct {
	data      []map[string]*pb.VarResult
	size      int
	head      int
	tail      int
	count     int
	consumers []uint32
}

func NewCyclicBuffer(size int) *CyclicBuffer {
	return &CyclicBuffer{
		data: make([]map[string]*pb.VarResult, size),
		size: size,
	}
}

func (cb *CyclicBuffer) Enqueue(item map[string]*pb.VarResult) error {
	if cb.count == cb.size {
		return fmt.Errorf("buffer is full")
	}
	cb.data[cb.tail] = item
	cb.tail = (cb.tail + 1) % cb.size
	cb.count++
	return nil
}

func (cb *CyclicBuffer) Dequeue() (map[string]*pb.VarResult, error) {
	if cb.count == 0 {
		return nil, fmt.Errorf("buffer is empty")
	}
	item := cb.data[cb.head]
	cb.head = (cb.head + 1) % cb.size
	cb.count--
	return item, nil
}

func (cb *CyclicBuffer) Peek() (map[string]*pb.VarResult, error) {
	if cb.count == 0 {
		return nil, fmt.Errorf("buffer is empty")
	}
	return cb.data[cb.head], nil
}

// gRPC Handlers
func (server *ResultsHubServer) ClaimCellFinished(ctx context.Context, in *pb.VarResults) (*pb.Empty, error) {
	log.Printf("[RECEIVED] claim from cell_%d\n", in.CellNumber)
	server.claimCellFinishedChan <- in
	<-server.claimAcknowledgementChan // this makes sure everything is stored into disk
	log.Printf("[SENDING] acknowlege the results from cell_%d\n", in.CellNumber)
	return &pb.Empty{}, nil
}

func (server *ResultsHubServer) FetchVarResult(ctx context.Context, in *pb.FetchVarResultRequest) (*pb.VarResult, error) {
	log.Printf("[RECEIVED] request fetching %s writen by cell_%d\n", in.VarName, in.VarAncestorCell)
	var varResult *pb.VarResult
	for {
		server.fetchVarRequestChan <- in
		varResult = <-server.fetchVarReplyChan
		if varResult.Available {
			break
		}
		// If the variable is not available yet, this handler will be busy waiting
		// Doing waiting on the server end simplifies the code on the user end
		time.Sleep(time.Second)
	}
	return varResult, nil
}

func (server *ResultsHubServer) WaitForCell(ctx context.Context, in *pb.WaitCellRequest) (*pb.Empty, error) {
	log.Printf("[RECEIVED] request for waiting cell%d\n", in.WaitFor)
	for {
		server.waitRequestChan <- in
		if <-server.waitAcknowledgementChan {
			break
		}
		time.Sleep(time.Second)
	}
	log.Printf("[SENDING] acknowledge for waiting cell%d\n", in.WaitFor)
	return &pb.Empty{}, nil
}

func (server *ResultsHubServer) SubmitRecursiveVars(ctx context.Context, in *pb.VarResults) (*pb.Empty, error) {
	log.Printf("[RECEIVED] submit recursive vars from cell_%d\n", in.CellNumber)
	if len(in.VarResuls) == 0 {
		// if there's no vars, this is pointless and will get an error
		return nil, status.Errorf(codes.Internal, "no recursive vars provided, request ignored")
	}
	server.submitRecursiveVarsChan <- in
	if !<-server.replyRecursiveVarsChan {
		// in this case, the buffer is full so we should return an error
		// let the user decide re-submit or drop
		return nil, status.Errorf(codes.Internal, "buffer is full")
	}
	return &pb.Empty{}, nil
}

func (server *ResultsHubServer) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("[RECEIVED] testing message %v\n", in.GetMessage())
	return &pb.HelloReply{Message: "Hello " + in.GetSenderId()}, nil
}

type CellVarResults struct {
	// cellNumber is wanted redundancy, since we log this struct to disk
	CellNumber      uint32
	NameToResultMap map[string]*pb.VarResult
}

func getAppRootPath() string {
	if _, err := os.Stat("/app"); err == nil {
		// we are in a container
		if err := os.MkdirAll("/app/data/", os.ModePerm); err != nil {
			log.Fatalf("[ERROR] failed to create /app/data/ directory: %v", err)
		}
		return "/app/data/"
	}
	// we are running locally
	return "./"
}

func storeCellResultsIntoDisk(cellResult *CellVarResults) {
	workDirName := getAppRootPath()
	fileName := fmt.Sprintf("%scell_%d_var_results.bin", workDirName, cellResult.CellNumber) // Prefix with root directory
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("[ERROR] storage component failed to open file %s: %v\n", fileName, err)
	}
	defer file.Close()

	// Create a gob encoder to serialize and store into disk
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(*cellResult); err != nil {
		log.Fatalf("[ERROR] storage component failed to encode results of %s: %v\n", fileName, err)
	}

	// Flush to disk before returning
	if err := file.Sync(); err != nil {
		log.Fatalf("[ERROR] storage component failed to flush file %s to disk: %v\n", fileName, err)
	}
}

func resultsHubRoutine(cells map[uint32]*CellVarResults, buffers map[uint32]*CyclicBuffer, claimCellFinishedChan chan *pb.VarResults, claimAcknowledgementChan chan bool, fetchVarRequestChan chan *pb.FetchVarResultRequest, fetchVarReplyChan chan *pb.VarResult, waitRequestChan chan *pb.WaitCellRequest, waitAcknowledgementChan chan bool, submitRecursiveVarsChan chan *pb.VarResults, replyRecursiveVarsChan chan bool, streamCells map[uint32]uint32, cellAncestors map[uint32][]uint32, cellConsumers map[uint32][]uint32) {
	for {
		select {
		case results := <-claimCellFinishedChan:
			// firstly need to check if this is a recursive cell (by streamInfo)
			// put into the buffer
			// scan through the previous buffers to if we can clean it up
			// TODO: of course this also cannot be hard coded, NEED: 1) each recursive variable is used by how many other cells, 2) which cells are consuming recursive variables
			_, isStream := streamCells[results.CellNumber]
			if isStream {
				// prepare the cyclic buffer
				cb, ok := buffers[results.CellNumber]
				if !ok {
					cb = NewCyclicBuffer(15) // CONFIGURE: can change this size
					cb.consumers = cellConsumers[results.CellNumber]
					buffers[results.CellNumber] = cb
				}

				// build varMap for further bookkeeping
				varMap := make(map[string]*pb.VarResult)
				for _, varRes := range results.VarResuls {
					varMap[varRes.VarName] = varRes
				}

				// try update the buffer
				if err := cb.Enqueue(varMap); err != nil {
					log.Printf("[WARN] Buffer full and dropped a submit request from cell%d.\n", results.CellNumber)
					claimAcknowledgementChan <- false
				} else {
					claimAcknowledgementChan <- true
				}

				// look for previous buffers to see if anything can be cleaned up
				for _, ancestor := range cellAncestors[results.CellNumber] {
					ancestorCb, cbOk := buffers[ancestor]
					if !cbOk {
						log.Fatalf("[ERROR] Received a submit whose ancestor don't have buffer registered. Bookkeeping corrupted!\n")
					}
					newconsumers := []uint32{}
					for _, consumer := range ancestorCb.consumers {
						if consumer != results.CellNumber {
							newconsumers = append(newconsumers, consumer)
						}
					}
					if len(newconsumers) == 0 {
						ancestorCb.Dequeue()
						ancestorCb.consumers = cellConsumers[results.CellNumber]
					}
				}

			} else {
				// then check if the cell record already exists
				_, ok := cells[results.CellNumber]
				if ok {
					// if this cell already submitted once,
					// later submissions from other pods are ignored
					claimAcknowledgementChan <- true
					continue
				}

				// build the varName to varResult map
				cells[results.CellNumber] = &CellVarResults{CellNumber: results.CellNumber, NameToResultMap: make(map[string]*pb.VarResult)}
				for _, varResult := range results.VarResuls {
					cells[results.CellNumber].NameToResultMap[varResult.VarName] = varResult
				}

				// serialize the CellVarResults of this cell and store into disk
				storeCellResultsIntoDisk(cells[results.CellNumber])
				claimAcknowledgementChan <- true
			}

		case request := <-fetchVarRequestChan:
			// firstly check if this variable is recursive by seeing the ancestor cell
			// then check if the ancestor buffer's first slot is still missing this result, if not block
			// TODO: this can't be hard coded of course, NEED: which cells are submitting recursive variables
			_, fromStream := streamCells[request.VarAncestorCell]
			if fromStream {
				buff, ok := buffers[request.VarAncestorCell]
				if !ok {
					// buffer doesn't exist, means the ancestor cell didn't finish for the first time
					fetchVarReplyChan <- &pb.VarResult{Available: false}
					continue
				}
				// check if this cell is not consumed yet
				canFetch := false
				for _, consumer := range buff.consumers {
					if consumer == request.Fetcher {
						canFetch = true
					}
				}
				if !canFetch {
					// this generation is already consmed
					fetchVarReplyChan <- &pb.VarResult{Available: false}
					continue
				}

				varMap, err := buff.Peek()
				if err != nil {
					// buffer exist but it is empty
					fetchVarReplyChan <- &pb.VarResult{Available: false}
					continue
				}

				varRes, varAvailable := varMap[request.VarName]
				if !varAvailable {
					log.Fatalf("[ERROR] Corruption!\n")
				}
				// fianlly the varRes is good and can be send back
				varRes.Available = true
				fetchVarReplyChan <- varRes
			} else {
				// check if the requested result is available
				cellResults, ok := cells[request.VarAncestorCell]
				if !ok {
					fetchVarReplyChan <- &pb.VarResult{Available: false}
					continue
				}

				// get the var
				theVar, ok := cellResults.NameToResultMap[request.VarName]
				if !ok {
					// if the variable is not actually there, THIS IS A SEVERE ISSUE OF JUP2KUB!
					log.Fatalf("[ERROR] Variable %s requested by cell_%d does not exist though the ancestor finished\n", request.VarName, request.VarAncestorCell)
				}

				theVar.Available = true
				fetchVarReplyChan <- theVar
			}

		case request := <-waitRequestChan:
			// check if the waitFor cell finished
			_, ok := cells[request.WaitFor]
			if !ok {
				waitAcknowledgementChan <- false
				continue
			}
			waitAcknowledgementChan <- true

		case recursiveVars := <-submitRecursiveVarsChan:
			// only the emitter process will send this rpc recursively
			cb, ok := buffers[recursiveVars.CellNumber]
			if !ok {
				cb = NewCyclicBuffer(15) // CONFIGURE: can change this size
				cb.consumers = cellConsumers[recursiveVars.CellNumber]
				buffers[recursiveVars.CellNumber] = cb
			}

			varMap := make(map[string]*pb.VarResult)
			for _, varRes := range recursiveVars.VarResuls {
				varMap[varRes.VarName] = varRes
			}

			if err := cb.Enqueue(varMap); err != nil {
				log.Printf("[WARN] Buffer full and dropped a submit request from cell%d.\n", recursiveVars.CellNumber)
				replyRecursiveVarsChan <- false
			} else {
				replyRecursiveVarsChan <- true
			}
		}
	}
}

/*
	---- HELPER FUNCTIONS ----
*/

func parseStreamCells() (map[uint32]uint32, error) {
	jsonpath := filepath.Join(getAppRootPath(), "streamInfo.json")
	data, err := os.ReadFile(jsonpath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("[INFO] StreamInfo does not exist, no streaming")
		}
		return nil, fmt.Errorf("[ERROR] Failed to read StreamInfo: %w", err)
	}

	var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, fmt.Errorf("[INFO] Failed to unmarshal JSON: %w", err)
	}

	// Navigate through the map to extract the "processor-cells" list.
	if streamProcessing, ok := jsonData["streamProcessing"].(map[string]interface{}); ok {
		if cells, ok := streamProcessing["processor-cells"].([]interface{}); ok {
			result := make(map[uint32]uint32)
			for _, cell := range cells {
				if num, ok := cell.(float64); ok { // JSON unmarshals numbers as float64 by default
					result[uint32(num)] = 0
				}
			}
			return result, nil
		}
	}
	return nil, fmt.Errorf("[ERROR] The structure of JSON does not match expected format")
}

func parseAncestors() (map[uint32][]uint32, error) {
	// Read the file content
	jsonpath := filepath.Join(getAppRootPath(), "streamInfo.json")
	data, err := os.ReadFile(jsonpath)
	if err != nil {
		return nil, fmt.Errorf("ERROR] Error reading file: %w", err)
	}

	tempMap := make(map[string][]uint32)
	err = json.Unmarshal(data, &tempMap)
	if err != nil {
		return nil, err
	}

	// Map to return, with integer keys
	dependencies := make(map[uint32][]uint32)
	for key, value := range tempMap {
		intKey, err := strconv.Atoi(key)
		if err != nil {
			return nil, err
		}
		dependencies[uint32(intKey)] = value
	}

	return dependencies, nil
}

type CellDependencies map[string]map[string][]string // temp struct for parsing script

func parseConsumers() (map[uint32][]uint32, error) {
	// Read the JSON file
	jsonpath := filepath.Join(getAppRootPath(), "all_relations.json")
	data, err := os.ReadFile(jsonpath)
	if err != nil {
		return nil, err
	}

	// Unmarshal JSON data into the structurally defined map
	var dependencies CellDependencies
	err = json.Unmarshal(data, &dependencies)
	if err != nil {
		return nil, err
	}

	// Process dependencies to generate the final map
	resultMap := make(map[uint32][]uint32)
	for key, valueMap := range dependencies {
		uintKey, err := strconv.ParseUint(key, 10, 32)
		if err != nil {
			return nil, err
		}

		uniqueCells := make(map[uint32]struct{}) // Use a set to avoid duplicates
		for _, cells := range valueMap {
			for _, cell := range cells {
				uintCell, err := strconv.ParseUint(cell, 10, 32)
				if err != nil {
					return nil, err
				}
				if uintCell != uintKey { // Ignore self-references
					uniqueCells[uint32(uintCell)] = struct{}{}
				}
			}
		}

		for cell := range uniqueCells {
			resultMap[uint32(uintKey)] = append(resultMap[uint32(uintKey)], cell)
		}
	}

	return resultMap, nil
}

func loadCellResultsFromDisk(cells map[uint32]*CellVarResults) {
	workDirName := getAppRootPath()
	entries, err := os.ReadDir(workDirName) // Read from root directory
	if err != nil {
		log.Fatalf("[ERROR] Failed to read directory: %v\n", err)
	}

	loaded := 0
	for _, entry := range entries {
		fileName := entry.Name()
		if strings.HasPrefix(fileName, "cell_") && strings.HasSuffix(fileName, "_var_results.bin") {
			var cellNumber uint32
			_, err := fmt.Sscanf(fileName, "cell_%d_var_results.bin", &cellNumber)
			if err != nil {
				log.Printf("[WARNING] Failed to parse cell number from file %s: %v\n", fileName, err)
				continue
			}

			file, err := os.Open(filepath.Join(workDirName, fileName)) // Open from root directory
			if err != nil {
				log.Printf("[WARNING] Failed to open file %s: %v\n", fileName, err)
				continue
			}

			decoder := gob.NewDecoder(file)
			var cellResult CellVarResults
			if err := decoder.Decode(&cellResult); err != nil {
				log.Printf("[WARNING] Failed to decode file %s: %v\n", fileName, err)
				file.Close()
				continue
			}
			file.Close()

			cells[cellResult.CellNumber] = &cellResult
			loaded += 1
		}
	}
	log.Printf("[STARTING] Recovered %d results from disk", loaded)
}

func main() {
	// initialize the resultsHub's channels and records
	cells := make(map[uint32]*CellVarResults, 0)
	loadCellResultsFromDisk(cells)
	buffers := make(map[uint32]*CyclicBuffer)

	claimCellFinishedChan := make(chan *pb.VarResults, 1)
	claimAcknowledgementChan := make(chan bool, 1)
	fetchVarRequestChan := make(chan *pb.FetchVarResultRequest, 1)
	fetchVarReplyChan := make(chan *pb.VarResult, 1)
	waitRequestChan := make(chan *pb.WaitCellRequest, 1)
	waitAcknowledgementChan := make(chan bool, 1)
	submitRecursiveVarsChan := make(chan *pb.VarResults, 1)
	replyRecursiveVarsChan := make(chan bool, 1)

	// prepare streamProcessing metadata
	streamCells, errSC := parseStreamCells()
	cellAncestors, errCA := parseAncestors()
	cellConsumers, errCC := parseConsumers()
	if errSC == nil && errCA == nil && errCC == nil {
		log.Printf("[STARTING] ResultsHub received stream processing info.\n")
	} else {
		log.Printf("[STARTING] ResultsHub starting WITHOUT stream processing info.\n")
	}

	go resultsHubRoutine(cells, buffers, claimCellFinishedChan, claimAcknowledgementChan, fetchVarRequestChan, fetchVarReplyChan, waitRequestChan, waitAcknowledgementChan, submitRecursiveVarsChan, replyRecursiveVarsChan, streamCells, cellAncestors, cellConsumers)

	// listen on 50051 by default
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("[ERROR] failed to listen: %v", err)
	}
	// start the resultsHub server
	s := grpc.NewServer()
	pb.RegisterResultsHubServer(s, &ResultsHubServer{
		claimCellFinishedChan:    claimCellFinishedChan,
		claimAcknowledgementChan: claimAcknowledgementChan,
		fetchVarRequestChan:      fetchVarRequestChan,
		fetchVarReplyChan:        fetchVarReplyChan,
		waitRequestChan:          waitRequestChan,
		waitAcknowledgementChan:  waitAcknowledgementChan,
		submitRecursiveVarsChan:  submitRecursiveVarsChan,
		replyRecursiveVarsChan:   replyRecursiveVarsChan,
	})
	log.Printf("[STARTING] gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("[ERROR] failed to serve: %v", err)
	}
}
