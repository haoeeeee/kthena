/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/volcano-sh/kthena/pkg/kthena-router/accesslog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

var (
	accessLogTextSelectedPodRE = regexp.MustCompile(`selected_pod=([^\s]+)`)
	accessLogJSONSelectedPodRE = regexp.MustCompile(`"selected_pod"\s*:\s*"([^"]*)"`)
)

type selectedBackend struct {
	pod         string
	modelServer string
}

// SelectedPodAfterChatURLHeaders sends a chat request and reads selected_pod from router logs.
func SelectedPodAfterChatURLHeaders(t *testing.T, kube kubernetes.Interface, kthenaNamespace, url, modelName string, messages []ChatMessage, extra map[string]string) string {
	t.Helper()
	return selectedBackendAfterChatURLHeaders(t, kube, kthenaNamespace, url, modelName, messages, extra).pod
}

func selectedBackendAfterChatURLHeaders(t *testing.T, kube kubernetes.Interface, kthenaNamespace, url, modelName string, messages []ChatMessage, extra map[string]string) selectedBackend {
	t.Helper()
	requestID := "e2e-router-" + RandomString(16)
	headers := map[string]string{"x-request-id": requestID}
	for k, v := range extra {
		headers[k] = v
	}
	_ = CheckChatCompletionsWithURLAndHeaders(t, url, modelName, messages, headers)

	ctx := context.Background()
	deadline := time.Now().Add(45 * time.Second)
	tailLines := int64(4000)
	for time.Now().Before(deadline) {
		for _, pod := range GetReadyRouterPods(t, kube, kthenaNamespace) {
			tl := tailLines
			raw, err := kube.CoreV1().Pods(kthenaNamespace).GetLogs(pod.Name, &corev1.PodLogOptions{TailLines: &tl}).Do(ctx).Raw()
			if err != nil {
				continue
			}
			for _, line := range strings.Split(string(raw), "\n") {
				if backend := selectedBackendFromAccessLogLine(line, requestID); backend.pod != "" {
					return backend
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "selected_pod not found in router access logs", "request_id=%s", requestID)
	return selectedBackend{}
}

func selectedBackendFromAccessLogLine(line, requestID string) selectedBackend {
	entry, ok := ParseRouterAccessLogLine([]byte(line))
	if ok && entry.RequestID == requestID && entry.SelectedPod != "" {
		return selectedBackend{pod: entry.SelectedPod, modelServer: entry.ModelServer}
	}
	// Fallback for lines that only match the request-id + selected_pod regexes.
	if pod := selectedPodFromAccessLogLine(line, requestID); pod != "" {
		return selectedBackend{pod: pod}
	}
	return selectedBackend{}
}

func selectedPodFromAccessLogLine(line, requestID string) string {
	if !strings.Contains(line, "request_id="+requestID) &&
		!strings.Contains(line, `"request_id":"`+requestID+`"`) {
		return ""
	}
	if m := accessLogTextSelectedPodRE.FindStringSubmatch(line); len(m) > 1 {
		return m[1]
	}
	if m := accessLogJSONSelectedPodRE.FindStringSubmatch(line); len(m) > 1 {
		return m[1]
	}
	return ""
}

// ParseRouterAccessLogLine parses either supported Router access-log format.
func ParseRouterAccessLogLine(line []byte) (accesslog.AccessLogEntry, bool) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return accesslog.AccessLogEntry{}, false
	}
	if line[0] == '{' {
		var entry accesslog.AccessLogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return accesslog.AccessLogEntry{}, false
		}
		return entry, isRouterAccessLogEntry(entry)
	}
	return parseTextRouterAccessLog(string(line))
}

func parseTextRouterAccessLog(line string) (accesslog.AccessLogEntry, bool) {
	closingBracket := strings.IndexByte(line, ']')
	firstQuote := strings.IndexByte(line, '"')
	if closingBracket < 1 || firstQuote < closingBracket {
		return accesslog.AccessLogEntry{}, false
	}
	secondQuoteOffset := strings.IndexByte(line[firstQuote+1:], '"')
	if secondQuoteOffset < 0 {
		return accesslog.AccessLogEntry{}, false
	}
	secondQuote := firstQuote + 1 + secondQuoteOffset

	requestFields := strings.Fields(line[firstQuote+1 : secondQuote])
	remainder := strings.Fields(line[secondQuote+1:])
	if len(requestFields) != 3 || len(remainder) == 0 {
		return accesslog.AccessLogEntry{}, false
	}
	statusCode, err := strconv.Atoi(remainder[0])
	if err != nil {
		return accesslog.AccessLogEntry{}, false
	}
	entry := accesslog.AccessLogEntry{
		Method:     requestFields[0],
		Path:       requestFields[1],
		Protocol:   requestFields[2],
		StatusCode: statusCode,
	}
	entry.Timestamp, _ = time.Parse(time.RFC3339Nano, line[1:closingBracket])

	// String values may be quoted, and a quoted one can hold spaces, so read them
	// by field name rather than by splitting the line on whitespace.
	for name, target := range map[string]*string{
		"model_name":     &entry.ModelName,
		"model_route":    &entry.ModelRoute,
		"model_server":   &entry.ModelServer,
		"selected_pod":   &entry.SelectedPod,
		"request_id":     &entry.RequestID,
		"backend_type":   &entry.BackendType,
		"backend_name":   &entry.BackendName,
		"upstream_model": &entry.UpstreamModel,
		"error_origin":   &entry.ErrorOrigin,
		"gateway":        &entry.Gateway,
		"http_route":     &entry.HTTPRoute,
		"inference_pool": &entry.InferencePool,
	} {
		if value, ok := textAccessLogField(line, name); ok {
			*target = unquoteTextAccessLogValue(value)
		}
	}

	for _, field := range remainder[1:] {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch key {
		case "upstream_status_code":
			entry.UpstreamStatusCode, _ = strconv.Atoi(value)
		case "upstream_attempts":
			entry.UpstreamAttempts, _ = strconv.Atoi(value)
		case "tokens":
			_, _ = fmt.Sscanf(value, "%d/%d", &entry.InputTokens, &entry.OutputTokens)
		case "timings":
			_, _ = fmt.Sscanf(value, "%dms(%d+%d+%d)",
				&entry.DurationTotal,
				&entry.DurationRequestProcessing,
				&entry.DurationUpstreamProcessing,
				&entry.DurationResponseProcessing,
			)
		}
	}
	if value, ok := textAccessLogField(line, "error"); ok {
		errorType, message, _ := strings.Cut(unquoteTextAccessLogValue(value), ":")
		entry.Error = &accesslog.ErrorInfo{Type: errorType, Message: message}
	}
	return entry, isRouterAccessLogEntry(entry)
}

var textAccessLogFieldNames = []string{
	"error",
	"model_name",
	"model_route",
	"model_server",
	"selected_pod",
	"request_id",
	"backend_type",
	"backend_name",
	"upstream_model",
	"upstream_status_code",
	"upstream_attempts",
	"error_origin",
	"gateway",
	"http_route",
	"inference_pool",
	"tokens",
	"timings",
}

func unquoteTextAccessLogValue(value string) string {
	if !strings.HasPrefix(value, `"`) {
		return value
	}
	if unquoted, err := strconv.Unquote(value); err == nil {
		return unquoted
	}
	return value
}

func textAccessLogField(line, name string) (string, bool) {
	marker := " " + name + "="
	start := strings.Index(line, marker)
	if start < 0 {
		return "", false
	}
	value := line[start+len(marker):]
	end := len(value)
	for _, nextName := range textAccessLogFieldNames {
		next := strings.Index(value, " "+nextName+"=")
		if next >= 0 && next < end {
			end = next
		}
	}
	return value[:end], true
}

func isRouterAccessLogEntry(entry accesslog.AccessLogEntry) bool {
	return entry.Method != "" && entry.Path != "" && entry.Protocol != "" && entry.StatusCode > 0
}
