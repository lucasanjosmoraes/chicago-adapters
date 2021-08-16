package fasthttpadapter

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lucasanjosmoraes/chicago-ports/pkg/http"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/log"
	routing "github.com/qiangxue/fasthttp-routing"
	"github.com/valyala/fasthttp"
)

// ServerConfig stores all server configuration variables.
type ServerConfig struct {
	Port string
}

// ServerAdapter implements http.Server, using the fasthttp lib.
type ServerAdapter struct {
	Logger log.Logger
	server *fasthttp.Server
	port   int
}

// NewServer instantiates a new ServerAdapter, but also helps us to validate if
// ServerAdapter implements http.Server.
func NewServer(ctx context.Context, l log.Logger, c ServerConfig) http.Server {
	port, err := strconv.Atoi(c.Port)
	if err != nil {
		l.Warnf(ctx, "invalid port: %s. Using default.", err)
	}

	if port == 0 {
		port = 3000
	}

	return ServerAdapter{
		Logger: l,
		port:   port,
		server: &fasthttp.Server{
			ReadTimeout:  time.Second * 30,
			WriteTimeout: time.Second * 30,
			IdleTimeout:  time.Second * 30,
		},
	}
}

// Listen will load all the handlers in a new router and will listen and serve the
// endpoints in the port attribute from the ServerAdapter.
func (a ServerAdapter) Listen(ctx context.Context, router *http.Router) error {
	r := routing.New()

	for _, h := range router.Handlers {
		r.To(h.Method, fixPath(h.Path), a.handler(h))
	}

	a.server.Handler = r.HandleRequest

	a.Logger.Infof(ctx, "Listening on port: ", strconv.Itoa(a.port))
	return a.server.ListenAndServe(fmt.Sprintf(":%d", a.port))
}

// Stop will shutdown the server.
func (a ServerAdapter) Stop(_ context.Context) error {
	err := a.server.Shutdown()
	if err != nil {
		return fmt.Errorf("could not shutdown the server: %s", err)
	}

	return nil
}

// StopError returns an error from context, if any.
func (a ServerAdapter) StopError() error {
	return context.Canceled
}

func (a ServerAdapter) handler(h http.Handler) func(c *routing.Context) error {
	return func(c *routing.Context) error {
		r := Request{Context: c}
		res := Response{Context: c.RequestCtx}

		h.Handler(c, r, res)

		return nil
	}
}

func fixPath(url string) string {
	url = strings.Replace(url, "{", "<", -1)
	url = strings.Replace(url, "}", ">", -1)

	return url
}
