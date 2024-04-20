package cluster_database

import (
	"context"

	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/client"
)

// ConnectionFactory is a factory to create connection
type connectionFactory struct {
	Peer string // Peer is the address of the peer
}

func newConnectionFactory(peer string) pool.PooledObjectFactory {
	return &connectionFactory{Peer: peer}
}

func (factory *connectionFactory) MakeObject(_ context.Context) (*pool.PooledObject, error) {
	// MakeObject creates a new connectionFactory
	oneClient, err := client.NewClient(factory.Peer)
	if err != nil {
		return nil, err
	}
	oneClient.Start()
	return pool.NewPooledObject(oneClient), nil
}

func (factory *connectionFactory) DestroyObject(_ context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(*client.Client)
	if !ok {
		return TYPE_MISMATCH
	}
	c.Close()

	return nil
}

func (factory *connectionFactory) ValidateObject(_ context.Context, _ *pool.PooledObject) bool {
	return true
}

func (factory *connectionFactory) ActivateObject(_ context.Context, _ *pool.PooledObject) error {
	return nil
}

func (factory *connectionFactory) PassivateObject(_ context.Context, _ *pool.PooledObject) error {
	return nil
}
