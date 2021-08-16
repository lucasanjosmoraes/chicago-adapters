package nethttpadapter

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	internalhttp "github.com/lucasanjosmoraes/chicago-ports/pkg/http"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/log"
)

// ServerAdapter implements http.Server, using the net/http pkg.
type ServerAdapter struct {
	Logger log.Logger
	server *http.Server
	port   int
}

// ServerConfig stores all server configuration variables.
type ServerConfig struct {
	Port string
}

// NewServer instantiates a new ServerAdapter, but also helps us to validate if
// ServerAdapter implements http.Server.
func NewServer(ctx context.Context, l log.Logger, c ServerConfig) internalhttp.Server {
	port, err := strconv.Atoi(c.Port)
	if err != nil {
		l.Warn(ctx, "invalid port: %s. Using default.", err)
	}

	if port == 0 {
		port = 3000
	}

	return ServerAdapter{
		Logger: l,
		port:   port,
		server: &http.Server{
			Addr:           fmt.Sprintf(":%d", port),
			ReadTimeout:    time.Second * 30,
			WriteTimeout:   time.Second * 30,
			IdleTimeout:    time.Second * 30,
			MaxHeaderBytes: 1 << 20,
		},
	}
}

// Listen will load all the handlers in a new router (from gorilla lib) and will
// listen and serve the endpoints in the port attribute from the ServerAdapter.
func (a ServerAdapter) Listen(ctx context.Context, router *internalhttp.Router) error {
	r := mux.NewRouter()

	for _, h := range router.Handlers {
		r.HandleFunc(h.Path, a.handler(ctx, h)).Methods(h.Method)
	}
	for _, sh := range router.StaticHandlers {
		r.PathPrefix(sh.Path).Handler(a.httpHandler(sh.Path, sh.Root))
	}

	a.server.Handler = r

	a.Logger.Infof(ctx, "Listening on port :%d", a.port)
	return a.server.ListenAndServe()
}

// Stop will disable the keep alive flag and will shutdown the server.
func (a ServerAdapter) Stop(ctx context.Context) error {
	a.server.SetKeepAlivesEnabled(false)

	if err := a.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("could not shutdown the server: %s", err)
	}

	return nil
}

// StopError returns an error from context, if any.
func (a ServerAdapter) StopError() error {
	return http.ErrServerClosed
}

func (a ServerAdapter) handler(ctx context.Context, h internalhttp.Handler) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		r := Request{NetRequest: req}
		res := Response{ResponseWriter: rw}

		h.Handler(ctx, r, res)
	}
}

func (a ServerAdapter) httpHandler(path, root string) http.Handler {
	fs := http.FileServer(http.Dir(root))

	return http.StripPrefix(path, fs)
}
