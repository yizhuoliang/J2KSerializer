package main

import (
	"context"
	"log"
	"net"

	pb "github.com/yizhuoliang/J2KSerializer"

	"google.golang.org/grpc"
)

type brokerServer struct {
	pb.UnimplementedBrokerServiceServer

	claimCellFinishedChan chan *pb.VarResults
	fetchVarRequestChan   chan *pb.FetchVarResultRequest
	fetchVarReplyChan     chan *pb.VarResult
}

func (bs *brokerServer) ClaimCellFinished(ctx context.Context, in *pb.VarResults) (*pb.Empty, error) {
	bs.claimCellFinishedChan <- in
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
	}
	return varResult, nil
}

func (bs *brokerServer) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetMessage())
	return &pb.HelloReply{Message: "Hello " + in.GetSenderId()}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterBrokerServiceServer(s, &brokerServer{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
