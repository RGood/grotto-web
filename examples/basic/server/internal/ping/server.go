package ping

import (
	"context"
	"fmt"
	"io"

	pingpb "github.com/RGood/grotto-web/examples/basic/server/internal/gen/ping"
)

type Server struct {
	pingpb.UnimplementedPingServiceServer
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Ping(_ context.Context, req *pingpb.PingRequest) (*pingpb.PingResponse, error) {
	return &pingpb.PingResponse{
		Message: fmt.Sprintf("pong: %s", req.GetMessage()),
	}, nil
}

func (s *Server) PingServerStream(req *pingpb.PingRequest, stream pingpb.PingService_PingServerStreamServer) error {
	count := streamCount(req.GetCount())
	for i := uint32(1); i <= count; i++ {
		if err := stream.Send(&pingpb.PingResponse{
			Message:  fmt.Sprintf("pong: %s", req.GetMessage()),
			Sequence: i,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) PingClientStream(stream pingpb.PingService_PingClientStreamServer) error {
	var messages []string
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pingpb.PingResponse{
				Message: fmt.Sprintf("pong: [%s]", joinMessages(messages)),
			})
		}
		if err != nil {
			return err
		}
		messages = append(messages, req.GetMessage())
		if limit := req.GetCount(); limit > 0 && uint32(len(messages)) >= limit {
			return stream.SendAndClose(&pingpb.PingResponse{
				Message: fmt.Sprintf("pong: [%s]", joinMessages(messages)),
			})
		}
	}
}

func (s *Server) PingBidiStream(stream pingpb.PingService_PingBidiStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&pingpb.PingResponse{
			Message:  fmt.Sprintf("pong: %s", req.GetMessage()),
			Sequence: req.GetCount(),
		}); err != nil {
			return err
		}
	}
}

func streamCount(count uint32) uint32 {
	if count == 0 {
		return 3
	}
	return count
}

func joinMessages(messages []string) string {
	switch len(messages) {
	case 0:
		return ""
	case 1:
		return messages[0]
	default:
		out := messages[0]
		for _, m := range messages[1:] {
			out += ", " + m
		}
		return out
	}
}
