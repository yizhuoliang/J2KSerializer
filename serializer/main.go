package main

import (
	"context"
	"log"
	"net"
	"time"

	pb "github.com/yizhuoliang/J2KSerializer"

	"google.golang.org/grpc"
)

type brokerServer struct {
	pb.UnimplementedBrokerServiceServer

	claimCellFinishedChan   chan *pb.VarResults
	claimAcknowlegementChan chan *pb.Empty
	fetchVarRequestChan     chan *pb.FetchVarResultRequest
	fetchVarReplyChan       chan *pb.VarResult
}

// gRPC Handlers
func (bs *brokerServer) ClaimCellFinished(ctx context.Context, in *pb.VarResults) (*pb.Empty, error) {
	bs.claimCellFinishedChan <- in
	<-bs.claimAcknowlegementChan // this makes sure everything is stored into disk
	return &pb.Empty{}, nil
}

func (bs *brokerServer) FetchVarResult(ctx context.Context, in *pb.FetchVarResultRequest) (*pb.VarResult, error) {
	bs.fetchVarRequestChan <- in
	var varResult *pb.VarResult
	for {
		varResult = <-bs.fetchVarReplyChan
		if varResult.Available {
			break
		}
		// If the variable is not available yet, this handler will be busy waiting
		// Doing waiting on the server end simplifies the code on the user end
		time.Sleep(time.Second)
	}
	return varResult, nil
}

func (bs *brokerServer) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetMessage())
	return &pb.HelloReply{Message: "Hello " + in.GetSenderId()}, nil
}

type CellVarResults struct {
	// cellNumber is wanted redundancy, since we log this struct to disk
	cellNumber      uint32
	nameToResultMap map[string]*pb.VarResult
}

func brokerRoutine(cells map[uint32]*CellVarResults, claimCellFinishedChan chan *pb.VarResults, claimAcknowlegementChan chan *pb.Empty, fetchVarRequestChan chan *pb.FetchVarResultRequest, fetchVarReplyChan chan *pb.VarResult) {
	for {
		select {
		case results := <-claimCellFinishedChan:
			// first check if the cell record already exists
			_, ok := cells[results.CellNumber]
			if ok {
				// if this cell already submitted once,
				// later submissions from other pods are ignored
				claimAcknowlegementChan <- &pb.Empty{}
			}

			// build the varName to varResult map
			cells[results.CellNumber] = &CellVarResults{cellNumber: results.CellNumber}
			for _, varResult := range results.VarResuls {
				cells[results.CellNumber].nameToResultMap[varResult.VarName] = varResult
			}

			// serialize the CellVarResults of this cell and store into disk
			storeCellResultsIntoDisk(cells[results.CellNumber])
		}
	}
}

func main() {
	// initialize the broker's channels and records
	cells := make(map[uint32]*CellVarResults, 0)
	claimCellFinishedChan := make(chan *pb.VarResults)
	claimAcknowlegementChan := make(chan *pb.Empty)
	fetchVarRequestChan := make(chan *pb.FetchVarResultRequest)
	fetchVarReplyChan := make(chan *pb.VarResult)

	// listen on 50051 by default
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// start the broker server
	s := grpc.NewServer()
	pb.RegisterBrokerServiceServer(s,
		&brokerServer{claimCellFinishedChan: claimCellFinishedChan,
			claimAcknowlegementChan: claimAcknowlegementChan,
			fetchVarRequestChan:     fetchVarRequestChan,
			fetchVarReplyChan:       fetchVarReplyChan})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
