package clusterapi

import (
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/docker/swarm-v2/api"
	"github.com/docker/swarm-v2/manager/state"
	"github.com/stretchr/testify/assert"
)

type testServer struct {
	Server *Server
	Client api.ClusterClient

	grpcServer *grpc.Server
	clientConn *grpc.ClientConn
}

func (ts *testServer) Stop() {
	_ = ts.clientConn.Close()
	ts.grpcServer.Stop()
}

func newTestServer(t *testing.T) *testServer {
	ts := &testServer{}

	store := state.NewMemoryStore()
	assert.NotNil(t, store)
	ts.Server = NewServer(store)
	assert.NotNil(t, ts.Server)

	temp, err := ioutil.TempFile("", "test-socket")
	assert.NoError(t, err)
	assert.NoError(t, temp.Close())
	assert.NoError(t, os.Remove(temp.Name()))

	lis, err := net.Listen("unix", temp.Name())
	assert.NoError(t, err)

	ts.grpcServer = grpc.NewServer()
	api.RegisterClusterServer(ts.grpcServer, ts.Server)
	go func() {
		// Serve will always return an error (even when properly stopped).
		// Explictly ignore it.
		_ = ts.grpcServer.Serve(lis)
	}()

	conn, err := grpc.Dial(temp.Name(), grpc.WithInsecure(), grpc.WithTimeout(10*time.Second),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}))
	assert.NoError(t, err)

	ts.Client = api.NewClusterClient(conn)

	return ts
}