package raft

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "raftkv/proto"
)

// GRPCTransport sends Raft RPCs over gRPC using the configured peer addresses.
type GRPCTransport struct {
	addrs map[string]string
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewGRPCTransport builds a client-side transport. addrs maps peer id → host:port.
func NewGRPCTransport(addrs map[string]string) *GRPCTransport {
	return &GRPCTransport{
		addrs: addrs,
		conns: make(map[string]*grpc.ClientConn),
	}
}

// Close tears down cached client connections.
func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.conns {
		_ = c.Close()
		delete(t.conns, id)
	}
}

func (t *GRPCTransport) client(to string) (pb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, ok := t.conns[to]; ok {
		return pb.NewRaftClient(conn), nil
	}
	addr, ok := t.addrs[to]
	if !ok || addr == "" {
		return nil, fmt.Errorf("%w: unknown peer %s", ErrUnreachable, to)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[to] = conn
	return pb.NewRaftClient(conn), nil
}

func (t *GRPCTransport) SendRequestVote(to string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.RequestVote(ctx, voteReqToProto(req))
	if err != nil {
		return nil, err
	}
	return voteRespFromProto(resp), nil
}

func (t *GRPCTransport) SendAppendEntries(to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.AppendEntries(ctx, appendReqToProto(req))
	if err != nil {
		return nil, err
	}
	return appendRespFromProto(resp), nil
}

type raftGRPCServer struct {
	pb.UnimplementedRaftServer
	node *RaftNode
}

func (s *raftGRPCServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	resp := s.node.HandleRequestVote(voteReqFromProto(req))
	return voteRespToProto(resp), nil
}

func (s *raftGRPCServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	resp := s.node.HandleAppendEntries(appendReqFromProto(req))
	return appendRespToProto(resp), nil
}

// RegisterRaftServer attaches this node's Raft handlers to a gRPC server.
func RegisterRaftServer(gs *grpc.Server, node *RaftNode) {
	pb.RegisterRaftServer(gs, &raftGRPCServer{node: node})
}

// ListenAndServeGRPC starts a gRPC server on addr and registers Raft (and any extra registrars).
func ListenAndServeGRPC(addr string, node *RaftNode, extra func(*grpc.Server)) (*grpc.Server, net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	gs := grpc.NewServer()
	RegisterRaftServer(gs, node)
	if extra != nil {
		extra(gs)
	}
	go func() {
		_ = gs.Serve(ln)
	}()
	return gs, ln, nil
}

func voteReqToProto(r *RequestVoteRequest) *pb.RequestVoteRequest {
	return &pb.RequestVoteRequest{
		Term:         int64(r.Term),
		CandidateId:  r.CandidateID,
		LastLogIndex: int64(r.LastLogIndex),
		LastLogTerm:  int64(r.LastLogTerm),
	}
}

func voteReqFromProto(r *pb.RequestVoteRequest) *RequestVoteRequest {
	return &RequestVoteRequest{
		Term:         int(r.Term),
		CandidateID:  r.CandidateId,
		LastLogIndex: int(r.LastLogIndex),
		LastLogTerm:  int(r.LastLogTerm),
	}
}

func voteRespToProto(r *RequestVoteResponse) *pb.RequestVoteResponse {
	return &pb.RequestVoteResponse{Term: int64(r.Term), VoteGranted: r.VoteGranted}
}

func voteRespFromProto(r *pb.RequestVoteResponse) *RequestVoteResponse {
	return &RequestVoteResponse{Term: int(r.Term), VoteGranted: r.VoteGranted}
}

func appendReqToProto(r *AppendEntriesRequest) *pb.AppendEntriesRequest {
	entries := make([]*pb.LogEntry, len(r.Entries))
	for i, e := range r.Entries {
		entries[i] = &pb.LogEntry{Term: int64(e.Term), Index: int64(e.Index), Command: e.Command}
	}
	return &pb.AppendEntriesRequest{
		Term:         int64(r.Term),
		LeaderId:     r.LeaderID,
		PrevLogIndex: int64(r.PrevLogIndex),
		PrevLogTerm:  int64(r.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int64(r.LeaderCommit),
	}
}

func appendReqFromProto(r *pb.AppendEntriesRequest) *AppendEntriesRequest {
	entries := make([]LogEntry, len(r.Entries))
	for i, e := range r.Entries {
		cmd := append([]byte(nil), e.Command...)
		entries[i] = LogEntry{Term: int(e.Term), Index: int(e.Index), Command: cmd}
	}
	return &AppendEntriesRequest{
		Term:         int(r.Term),
		LeaderID:     r.LeaderId,
		PrevLogIndex: int(r.PrevLogIndex),
		PrevLogTerm:  int(r.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int(r.LeaderCommit),
	}
}

func appendRespToProto(r *AppendEntriesResponse) *pb.AppendEntriesResponse {
	return &pb.AppendEntriesResponse{Term: int64(r.Term), Success: r.Success}
}

func appendRespFromProto(r *pb.AppendEntriesResponse) *AppendEntriesResponse {
	return &AppendEntriesResponse{Term: int(r.Term), Success: r.Success}
}
