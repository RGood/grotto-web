package main

import (
	"log"
	"net"

	pingpb "github.com/RGood/grotto-web/examples/basic/server/internal/gen/ping"
	"github.com/RGood/grotto-web/examples/basic/server/internal/ping"
	"github.com/RGood/grotto-web/server/pkg/grottoserver"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	server := grottoserver.NewServer(grottoserver.WithLogging(true))
	pingpb.RegisterPingServiceServer(server, ping.NewServer())

	log.Printf("ping service listening on %s", lis.Addr())
	if err := server.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
