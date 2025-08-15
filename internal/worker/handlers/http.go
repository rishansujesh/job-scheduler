package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type HTTPArgs struct {
	Method       string            `json:"method"`
	URL          string            `json:"url"`
	Headers      map[string]string `json:"headers,omitempty"`
	Body         any               `json:"body,omitempty"`
	TimeoutMS    int               `json:"timeout_ms,omitempty"`
	RetryOnCodes []int             `json:"retry_on_codes,omitempty"`
}

func RunHTTP(ctx context.Context, a HTTPArgs) (Result, error) {
	if a.Method == "" {
		a.Method = "GET"
	}
	if a.URL == "" {
		return Result{}, fmt.Errorf("http: url required")
	}
	to := time.Duration(a.TimeoutMS) * time.Millisecond
	if to <= 0 {
		to = 10 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()

	var bodyReader io.Reader
	if a.Body != nil {
		b, err := json.Marshal(a.Body)
		if err != nil {
			return Result{}, fmt.Errorf("http: body marshal: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(cctx, a.Method, a.URL, bodyReader)
	if err != nil {
		return Result{}, fmt.Errorf("http: new request: %w", err)
	}
	for k, v := range a.Headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" && a.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: to}
	resp, err := client.Do(req)
	if err != nil {
		return Result{Retryable: true}, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	res := Result{
		Stdout:    string(respBody),
		Retryable: false,
	}

	// Retry on configured status codes
	for _, c := range a.RetryOnCodes {
		if resp.StatusCode == c {
			res.Retryable = true
			break
		}
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return res, nil
	}
	return res, fmt.Errorf("http: status %d", resp.StatusCode)
}
