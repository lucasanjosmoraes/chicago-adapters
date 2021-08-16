package fasthttpadapter

import "github.com/valyala/fasthttp"

// Response implements http.Response.
type Response struct {
	Context *fasthttp.RequestCtx
}

func (r Response) Write(status int, body []byte) error {
	return writeCommon(r.Context, status, body)
}

func (r Response) WriteOK(body []byte) error {
	return writeCommon(r.Context, fasthttp.StatusOK, body)
}

func (r Response) WriteCreated(body []byte) error {
	return writeCommon(r.Context, fasthttp.StatusCreated, body)
}

func (r Response) WriteNoContent(body []byte) error {
	return writeCommon(r.Context, fasthttp.StatusNoContent, body)
}

func (r Response) WriteBadRequest(body []byte) error {
	return writeCommon(r.Context, fasthttp.StatusBadRequest, body)
}

func (r Response) WriteUnauthorized(body []byte) error {
	return writeCommon(r.Context, fasthttp.StatusUnauthorized, body)
}

func (r Response) WriteForbidden(body []byte) error {
	return writeCommon(r.Context, fasthttp.StatusForbidden, body)
}

func (r Response) WriteNotfound(body []byte) error {
	return writeCommon(r.Context, fasthttp.StatusNotFound, body)
}

func (r Response) WriteInternalError(body []byte) error {
	return writeCommon(r.Context, fasthttp.StatusInternalServerError, body)
}

func (r Response) JsonResponse() {
	r.Context.Response.Header.Set("Content-Type", "application/json")
}

func (r Response) XMLResponse() {
	r.Context.Response.Header.Set("Content-Type", "application/xml")
}

func writeCommon(ctx *fasthttp.RequestCtx, status int, body []byte) error {
	ctx.SetStatusCode(status)
	_, err := ctx.Write(body)

	return err
}
