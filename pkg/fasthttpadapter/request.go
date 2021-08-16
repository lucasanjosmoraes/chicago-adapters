package fasthttpadapter

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/url"

	routing "github.com/qiangxue/fasthttp-routing"
)

// Request implements http.Request.
type Request struct {
	Context *routing.Context
}

func (r Request) Body() io.ReadCloser {
	body := r.Context.PostBody()
	reader := bytes.NewReader(body)

	return ioutil.NopCloser(reader)
}

func (r Request) BodyBytes() []byte {
	return r.Context.PostBody()
}

func (r Request) Url() string {
	return r.Context.URI().String()
}

func (r Request) Header(name string) string {
	return string(r.Context.Request.Header.Peek(name))
}

func (r Request) Param(key string) string {
	return r.Context.Param(key)
}

func (r Request) Query(key string) string {
	v, err := url.ParseQuery(string(r.Context.URI().QueryString()))
	if err != nil {
		return ""
	}

	return v.Get(key)
}
