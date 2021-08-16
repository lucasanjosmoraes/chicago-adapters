package nethttpadapter

import (
	"io"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
)

// Request implements http.Request.
type Request struct {
	NetRequest *http.Request
}

func (r Request) Body() io.ReadCloser {
	return r.NetRequest.Body
}

func (r Request) BodyBytes() []byte {
	data, err := ioutil.ReadAll(r.NetRequest.Body)
	if err != nil {
		return make([]byte, 0)
	}

	defer func() {
		_ = r.NetRequest.Body.Close()
	}()

	return data
}

func (r Request) Url() string {
	return r.NetRequest.URL.String()
}

func (r Request) Header(name string) string {
	return r.NetRequest.Header.Get(name)
}

func (r Request) Param(key string) string {
	return mux.Vars(r.NetRequest)[key]
}

func (r Request) Query(key string) string {
	return r.NetRequest.URL.Query().Get(key)
}
