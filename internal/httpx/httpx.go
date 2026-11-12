package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details any    `json:"details,omitempty"`
	} `json:"error"`
}

func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func Error(w http.ResponseWriter, status int, code, msg string) {
	ErrorDetails(w, status, code, msg, nil)
}

func ErrorDetails(w http.ResponseWriter, status int, code, msg string, details any) {
	var b errorBody
	b.Error.Code, b.Error.Message, b.Error.Details = code, msg, details
	JSON(w, status, b)
}

func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		var syntax *json.SyntaxError
		var typ *json.UnmarshalTypeError
		switch {
		case errors.As(err, &tooBig):
			return fmt.Errorf("body exceeds %d bytes", maxBytes)
		case errors.As(err, &syntax):
			return fmt.Errorf("malformed JSON at offset %d", syntax.Offset)
		case errors.As(err, &typ):
			return fmt.Errorf("field %q has wrong type", typ.Field)
		case errors.Is(err, io.EOF):
			return errors.New("body is empty")
		default:
			return fmt.Errorf("invalid JSON: %w", err)
		}
	}
	if dec.More() {
		return errors.New("body must contain a single JSON object")
	}
	return nil
}

func Serve(ctx context.Context, srv *http.Server, log *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", srv.Addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	log.Info("http server shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func NewServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
