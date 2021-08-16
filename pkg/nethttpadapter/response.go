package nethttpadapter

import "net/http"

// Response implements http.Response.
type Response struct {
	ResponseWriter http.ResponseWriter
}

func (r Response) Write(status int, body []byte) error {
	return writeCommon(r.ResponseWriter, status, body)
}

func (r Response) WriteOK(body []byte) error {
	return writeCommon(r.ResponseWriter, http.StatusOK, body)
}

func (r Response) WriteCreated(body []byte) error {
	return writeCommon(r.ResponseWriter, http.StatusCreated, body)
}

func (r Response) WriteNoContent(body []byte) error {
	return writeCommon(r.ResponseWriter, http.StatusNoContent, body)
}

func (r Response) WriteBadRequest(body []byte) error {
	return writeCommon(r.ResponseWriter, http.StatusBadRequest, body)
}

func (r Response) WriteUnauthorized(body []byte) error {
	return writeCommon(r.ResponseWriter, http.StatusUnauthorized, body)
}

func (r Response) WriteForbidden(body []byte) error {
	return writeCommon(r.ResponseWriter, http.StatusForbidden, body)
}

func (r Response) WriteNotfound(body []byte) error {
	return writeCommon(r.ResponseWriter, http.StatusNotFound, body)
}

func (r Response) WriteInternalError(body []byte) error {
	return writeCommon(r.ResponseWriter, http.StatusInternalServerError, body)
}

func (r Response) JsonResponse() {
	r.ResponseWriter.Header().Set("Content-Type", "application/json")
}

func (r Response) XMLResponse() {
	r.ResponseWriter.Header().Set("Content-Type", "application/xml")
}

func writeCommon(writer http.ResponseWriter, status int, body []byte) error {
	writer.WriteHeader(status)
	_, err := writer.Write(body)

	return err
}
