package osenvsadapter

import (
	"context"
	"os"
	"strings"

	"github.com/lucasanjosmoraes/chicago-ports/pkg/config"
)

// Adapter implements config.Source, using the os pkg.
type Adapter struct {
	prefix string
}

// New instantiates a new Adapter, but also helps us to validate if it implements
// config.Source.
func New(prefix string) config.Source {
	return Adapter{
		prefix: prefix,
	}
}

// Load will search for all os env variables and will load them in a new map.
func (a Adapter) Load(_ context.Context) map[string]string {
	values := make(map[string]string)

	for _, env := range os.Environ() {
		sepEnv := strings.SplitN(env, "=", 2)
		k := sepEnv[0]
		v := sepEnv[1]

		if strings.HasPrefix(k, a.prefix) {
			k = strings.Replace(k, a.prefix, "", 1)
			values[k] = v
		}
	}

	return values
}
