package vaultadapter

import (
	"context"
	"strings"

	"github.com/lucasanjosmoraes/chicago-ports/pkg/config"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/log"
)

// Adapter implements config.Source, using the vault lib.
type Adapter struct {
	Logger log.Logger
	Config Config
	Client Client
}

// Config contains all attributes neeeded to connect with Vaut.
type Config struct {
	URL      string
	RoleID   string
	SecretID string
	Paths    string
}

// New instantiates a new Adapter, but also helps us to validate if it implements
// config.Source.
func New(l log.Logger, cfg Config) (config.Source, error) {
	c, err := NewClient(ClientConfig{
		URL:      cfg.URL,
		RoleID:   cfg.RoleID,
		SecretID: cfg.SecretID,
	})
	if err != nil {
		return Adapter{}, err
	}

	return Adapter{
		Logger: l,
		Config: cfg,
		Client: c,
	}, nil
}

// Load will search for all the variables stored on the defined paths on Vault and
// will load them in a new map.
func (a Adapter) Load(ctx context.Context) map[string]string {
	secrets := make(map[string]string)

	for _, p := range strings.Split(a.Config.Paths, ",") {
		kv, err := a.Client.ReadKV(p)
		if err != nil {
			a.Logger.Errorf(ctx, "error on read vault KV for path [%s]: %s", p, err)
			return nil
		}

		for k, v := range kv {
			secrets[k] = v
		}
	}

	return secrets
}
