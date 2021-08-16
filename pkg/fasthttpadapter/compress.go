package fasthttpadapter

import (
	"bytes"

	"github.com/valyala/fasthttp"
)

func getBodyEvenIfCompressed(resp *fasthttp.Response) ([]byte, error) {
	contentEncoding := resp.Header.Peek("Content-Encoding")

	if bytes.EqualFold(contentEncoding, []byte("gzip")) {
		return resp.BodyGunzip()
	}

	return resp.Body(), nil
}
