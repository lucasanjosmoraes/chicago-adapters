package mongoadapter

import (
	"context"
	"fmt"
	"time"

	"github.com/lucasanjosmoraes/chicago-ports/pkg/database"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Config contains all the attributes required to initialize the Adapter.
type Config struct {
	ConnectionString   string
	Database           string
	Username           string
	Password           string
	MaxPoolSize        int
	MinPoolSize        int
	InitialConnTimeout int
	MaxIdleConn        int
}

// Adapter implements database.MongoOpener to open connections to mongo databases,
// using the mongo-driver/mongo lib.
type Adapter struct {
	Config Config
}

// New instantiates a new Adapter, but also helps us to validate if it implements
// database.MongoOpener correctly.
func New(c Config) database.MongoOpener {
	return Adapter{
		Config: c,
	}
}

// Open will estabilish a connection to a mongo database using the defined configuration.
// Will also execute a ping to validate if the connection was correctly estabilished.
func (a Adapter) Open(ctx context.Context) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(a.Config.ConnectionString)
	clientOptions.SetAuth(options.Credential{
		AuthSource: a.Config.Database,
		Username:   a.Config.Username,
		Password:   a.Config.Password,
	})
	clientOptions.SetMaxPoolSize(uint64(a.Config.MaxPoolSize))
	clientOptions.SetMinPoolSize(uint64(a.Config.MinPoolSize))
	clientOptions.SetMaxConnIdleTime(time.Duration(a.Config.MaxIdleConn) * time.Second)
	clientOptions.SetConnectTimeout(time.Duration(a.Config.InitialConnTimeout) * time.Second)
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("mongo new client connection error: %s", err)
	}

	err = client.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("mongo client connection error: %s", err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(a.Config.InitialConnTimeout)*time.Second)
	defer cancel()

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, fmt.Errorf("mongo ping connection error: %s", err)
	}

	return client, nil
}
