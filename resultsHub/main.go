package main

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "github.com/yizhuoliang/J2KResultsHub"

	"google.golang.org/grpc"
)

type ResultsHubServer struct {
	pb.UnimplementedResultsHubServer

	claimCellFinishedChan   chan *pb.VarResults
	claimAcknowlegementChan chan *pb.Empty
	fetchVarRequestChan     chan *pb.FetchVarResultRequest
	fetchVarReplyChan       chan *pb.VarResult
}

// gRPC Handlers
func (server *ResultsHubServer) ClaimCellFinished(ctx context.Context, in *pb.VarResults) (*pb.Empty, error) {
	log.Printf("[RECEIVED] claim from cell_%d\n", in.CellNumber)
	server.claimCellFinishedChan <- in
	<-server.claimAcknowlegementChan // this makes sure everything is stored into disk
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
	if _, err := os.Stat("/app"); os.IsNotExist(err) {
		// /app does not exist, return "."
		return "./"
	}
	return "/app/"
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

func resultsHubRoutine(cells map[uint32]*CellVarResults, claimCellFinishedChan chan *pb.VarResults, claimAcknowlegementChan chan *pb.Empty, fetchVarRequestChan chan *pb.FetchVarResultRequest, fetchVarReplyChan chan *pb.VarResult) {
	for {
		select {
		case results := <-claimCellFinishedChan:
			// first check if the cell record already exists
			_, ok := cells[results.CellNumber]
			if ok {
				// if this cell already submitted once,
				// later submissions from other pods are ignored
				claimAcknowlegementChan <- &pb.Empty{}
				continue
			}

			// build the varName to varResult map
			cells[results.CellNumber] = &CellVarResults{CellNumber: results.CellNumber, NameToResultMap: make(map[string]*pb.VarResult)}
			for _, varResult := range results.VarResuls {
				cells[results.CellNumber].NameToResultMap[varResult.VarName] = varResult
			}

			// serialize the CellVarResults of this cell and store into disk
			storeCellResultsIntoDisk(cells[results.CellNumber])
			claimAcknowlegementChan <- &pb.Empty{}

		case request := <-fetchVarRequestChan:
			// check if the requested result is available
			cellResults, ok := cells[request.VarAncestorCell]
			if !ok {
				fetchVarReplyChan <- &pb.VarResult{Available: false}
				continue
			}

			// get the var
			theVar := cellResults.NameToResultMap[request.VarName]
			theVar.Available = true
			fetchVarReplyChan <- theVar
		}
	}
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
	claimCellFinishedChan := make(chan *pb.VarResults, 1)
	claimAcknowlegementChan := make(chan *pb.Empty, 1)
	fetchVarRequestChan := make(chan *pb.FetchVarResultRequest, 1)
	fetchVarReplyChan := make(chan *pb.VarResult, 1)

	go resultsHubRoutine(cells, claimCellFinishedChan, claimAcknowlegementChan,
		fetchVarRequestChan, fetchVarReplyChan)

	// listen on 50051 by default
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("[ERROR] failed to listen: %v", err)
	}
	// start the resultsHub server
	s := grpc.NewServer()
	pb.RegisterResultsHubServer(s,
		&ResultsHubServer{claimCellFinishedChan: claimCellFinishedChan,
			claimAcknowlegementChan: claimAcknowlegementChan,
			fetchVarRequestChan:     fetchVarRequestChan,
			fetchVarReplyChan:       fetchVarReplyChan})
	log.Printf("[STARTING] gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("[ERROR] failed to serve: %v", err)
	}
}
