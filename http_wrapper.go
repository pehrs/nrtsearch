package main

import (
  "context" // Use "golang.org/x/net/context" for Golang version <= 1.6
  "flag"
  "fmt"
  "net/http"
  "os"

  "github.com/golang/glog"
  "github.com/grpc-ecosystem/grpc-gateway/runtime"
  "google.golang.org/grpc"

  gw "github.com/Yelp/nrtsearch/grpc-gateway"
)

var (
  // command-line options:
  // gRPC server endpoint
	// example:
	//   grpc-gw localhost:6000 0.0.0.0:6080
	// or (in docker container):
	//   grpc-gw 172.17.0.1:6000 :6080
  grpc_server_hostport =  fmt.Sprintf("%s", os.Args[1])
  rest_server_hostport =  fmt.Sprintf("%s", os.Args[2])
  swagger_hostport =  fmt.Sprintf("%s", os.Args[3])
  grpcServerEndpoint = flag.String("grpc-server-endpoint",  grpc_server_hostport, "gRPC server endpoint")
)

func run() error {
  ctx := context.Background()
  ctx, cancel := context.WithCancel(ctx)
  defer cancel()

  // Register gRPC server endpoint
  // Note: Make sure the gRPC server is running properly and accessible
  mux := runtime.NewServeMux()
  runtime.SetHTTPBodyMarshaler(mux)

  opts := []grpc.DialOption{grpc.WithInsecure()}
  err := gw.RegisterLuceneServerHandlerFromEndpoint(ctx, mux,  *grpcServerEndpoint, opts)
  if err != nil {
    return err
  }

	swaggerUiFs := http.FileServer(http.Dir("./grpc-gateway/swaggerui"))
	http.Handle("/swaggerui/", http.StripPrefix("/swaggerui/", swaggerUiFs))
	grpcFs := http.FileServer(http.Dir("./grpc-gateway"))
	http.Handle("/grpc/", http.StripPrefix("/grpc/", grpcFs))

	go func() {
		// Spawn the Swagger server in `other` goroutine
		glog.Fatal(http.ListenAndServe(swagger_hostport, nil))
	}()

  // Start HTTP server (and proxy calls to gRPC server endpoint)
  return http.ListenAndServe(rest_server_hostport, mux)
}

func main() {
  flag.Parse()
  defer glog.Flush()

  if err := run(); err != nil {
    glog.Fatal(err)
  }
}
