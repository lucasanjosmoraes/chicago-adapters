package mssqldbadapter

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/database"
)

const schema = "sqlserver"

// Config contains all the attributes required to initialize the Adapter.
type Config struct {
	Host               string
	Port               string
	Database           string
	Username           string
	Password           string
	MaxIdleConn        int
	ConnMaxLifetimeSec int
	MaxOpenConn        int
}

// Adapter implements database.Opener to open connections to SQL Server databases,
// using the go-mssqldb lib.
type Adapter struct {
	Config Config
}

// New instantiates a new Adapter, but also helps us to validate if it implements
// database.Opener correctly.
func New(c Config) database.Opener {
	return Adapter{
		Config: c,
	}
}

// Open will estabilish a connection to a SQL server database using the defined configuration.
// Will also execute a ping to validate if the connection was correctly estabilished.
func (a Adapter) Open(ctx context.Context) (*sql.DB, error) {
	u := &url.URL{
		Scheme:     schema,
		User:       url.UserPassword(a.Config.Username, a.Config.Password),
		Host:       fmt.Sprintf("%s:%s", a.Config.Host, a.Config.Port),
		ForceQuery: false,
		RawQuery:   fmt.Sprintf("database=%s&connection+timeout=0", a.Config.Database),
	}

	db, err := sql.Open(schema, u.String())
	if err != nil {
		return nil, fmt.Errorf("open mssql connection error: %s", err)
	}

	db.SetMaxIdleConns(a.Config.MaxIdleConn)
	db.SetConnMaxLifetime(time.Second * time.Duration(a.Config.ConnMaxLifetimeSec))
	db.SetMaxOpenConns(a.Config.MaxOpenConn)

	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return db, nil
}
