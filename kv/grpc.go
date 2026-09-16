package kv

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	pb "raftkv/proto"
)

type kvGRPCServer struct {
	pb.UnimplementedKVServer
	srv *Server
}

// RegisterGRPC attaches the KV service to a gRPC server.
func RegisterGRPC(gs *grpc.Server, srv *Server) {
	pb.RegisterKVServer(gs, &kvGRPCServer{srv: srv})
}

func (k *kvGRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	v, found, err := k.srv.Get(req.Key)
	if err != nil {
		return redirectGet(err), nil
	}
	return &pb.GetResponse{Ok: true, Value: v, Found: found}, nil
}

func (k *kvGRPCServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if err := k.srv.Put(req.Key, req.Value); err != nil {
		resp := &pb.PutResponse{Ok: false, Error: err.Error()}
		var nl *NotLeaderError
		if errors.As(err, &nl) {
			resp.LeaderId = nl.LeaderID
			resp.LeaderAddr = nl.LeaderAddr
		}
		return resp, nil
	}
	return &pb.PutResponse{Ok: true}, nil
}

func (k *kvGRPCServer) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	st := k.srv.Status()
	return &pb.StatusResponse{
		Id:           st.ID,
		State:        st.State,
		Term:         int64(st.Term),
		LeaderId:     st.LeaderID,
		LeaderAddr:   st.LeaderAddr,
		VotedFor:     st.VotedFor,
		CommitIndex:  int64(st.CommitIndex),
		LastApplied:  int64(st.LastApplied),
		LastLogIndex: int64(st.LastLogIndex),
		KvSize:       int64(st.KVSize),
		Peers:        st.Peers,
	}, nil
}

func redirectGet(err error) *pb.GetResponse {
	resp := &pb.GetResponse{Ok: false, Error: err.Error()}
	var nl *NotLeaderError
	if errors.As(err, &nl) {
		resp.LeaderId = nl.LeaderID
		resp.LeaderAddr = nl.LeaderAddr
	}
	return resp
}
