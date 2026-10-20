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

package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/volcano-sh/kthena/pkg/kthena-router/accesslog"
)

const successfulExternalRequest = "successful_request"

// TestExternalModelProviders verifies each request at four observable boundaries:
// the downstream response, captured upstream request, metrics, and access log.
func TestExternalModelProviders(t *testing.T) {
	fixture := setupExternalProviderFixture(t, testCtx, testNamespace, kthenaNamespace)

	t.Run("OpenAIChatNonStreaming", fixture.testOpenAIChatNonStreaming)
	t.Run("OpenAIChatStreaming", fixture.testOpenAIChatStreaming)
	t.Run("OpenAICompletionsNonStreaming", fixture.testOpenAICompletionsNonStreaming)
	t.Run("OpenAICompletionsStreaming", fixture.testOpenAICompletionsStreaming)
	t.Run("OpenAIResponsesNonStreaming", fixture.testOpenAIResponsesNonStreaming)
	t.Run("OpenAIResponsesStreaming", fixture.testOpenAIResponsesStreaming)
	t.Run("AnthropicNonStreaming", fixture.testAnthropicNonStreaming)
	t.Run("AnthropicStreaming", fixture.testAnthropicStreaming)
	t.Run("AnthropicBearerGateway", fixture.testAnthropicBearerGateway)
	t.Run("NonTextInputAccounting", fixture.testNonTextInputAccounting)
	t.Run("Upstream429", fixture.testUpstream429)
	t.Run("ActiveGaugeLifecycle", fixture.testActiveGaugeLifecycle)
}

// testOpenAIChatNonStreaming covers tools and image content in a JSON response path.
func (f externalProviderFixture) testOpenAIChatNonStreaming(t *testing.T) {
	path := externalOpenAIChatPath
	before := f.readMetricSnapshot(t, externalOpenAIChatModel, path, http.StatusOK, successfulExternalRequest, testNamespace, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel)
	exchange := f.roundTrip(t, openAIChatRequest(externalOpenAIChatModel, `{
		"messages":[
			{"role":"user","content":[
				{"type":"text","text":"Use the image and tool."},
				{"type":"image_url","image_url":{"url":"data:image/png;base64,aW1hZ2U="}}
			]},
			{"role":"assistant","tool_calls":[{"id":"call-weather","type":"function","function":{"name":"weather","arguments":"{\"city\":\"Shanghai\"}"}}]},
			{"role":"tool","tool_call_id":"call-weather","content":"sunny"}
		],
		"tools":[{"type":"function","function":{"name":"weather","description":"Get weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}],
		"tool_choice":"auto",
		"stream":false,
		"max_tokens":16
	}`))
	response := exchange.response
	require.Equal(t, http.StatusOK, response.statusCode, "response: %s", response.body)
	assert.Equal(t, "application/json", response.header.Get("Content-Type"))
	var responseObject map[string]any
	require.NoError(t, json.Unmarshal(response.body, &responseObject))
	assert.Equal(t, "chat-mock", responseObject["id"])

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, false)
	assertRewrittenExternalPayload(t, exchange.requestBody, capture.Body, externalOpenAIChatUpstreamModel, false)

	after := f.waitForMetricDelta(t, before, externalOpenAIChatModel, path, http.StatusOK, successfulExternalRequest, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 5)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assertSuccessfulExternalAccessLog(t, entry, externalOpenAIChatModel, path, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 5)
	assertPositiveExternalInputAccounting(t, before, after, entry)
}

// testOpenAIChatStreaming verifies that the Router preserves existing stream options,
// injects usage collection, and forwards the terminating OpenAI SSE event.
func (f externalProviderFixture) testOpenAIChatStreaming(t *testing.T) {
	path := externalOpenAIChatPath
	before := f.readMetricSnapshot(t, externalOpenAIChatModel, path, http.StatusOK, successfulExternalRequest, testNamespace, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel)
	exchange := f.roundTrip(t, openAIChatRequest(externalOpenAIChatModel, `{
		"messages":[{"role":"user","content":"Stream a short answer."}],
		"stream":true,
		"stream_options":{"vendor_option":"preserve-me"},
		"max_tokens":16
	}`))
	response := exchange.response
	require.Equal(t, http.StatusOK, response.statusCode, "response: %s", response.body)
	assert.Contains(t, response.header.Get("Content-Type"), "text/event-stream")
	assert.Contains(t, string(response.body), `"object":"chat.completion.chunk"`)
	assert.Contains(t, string(response.body), `data: [DONE]`)

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, false)
	assertRewrittenExternalPayload(t, exchange.requestBody, capture.Body, externalOpenAIChatUpstreamModel, true)

	after := f.waitForMetricDelta(t, before, externalOpenAIChatModel, path, http.StatusOK, successfulExternalRequest, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 5)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assertSuccessfulExternalAccessLog(t, entry, externalOpenAIChatModel, path, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 5)
	assertPositiveExternalInputAccounting(t, before, after, entry)
}

// testOpenAICompletionsNonStreaming ensures the legacy Completions path remains
// free of streaming-only fields while still reporting provider output usage.
func (f externalProviderFixture) testOpenAICompletionsNonStreaming(t *testing.T) {
	path := externalOpenAICompletionsPath
	before := f.readMetricSnapshot(t, externalOpenAIChatModel, path, http.StatusOK, successfulExternalRequest, testNamespace, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel)
	exchange := f.roundTrip(t, openAICompletionsRequest(externalOpenAIChatModel, `{
		"prompt":"Complete this sentence.",
		"stream":false,
		"max_tokens":16
	}`))
	response := exchange.response
	require.Equal(t, http.StatusOK, response.statusCode, "response: %s", response.body)
	assert.Equal(t, "application/json", response.header.Get("Content-Type"))
	var responseObject map[string]any
	require.NoError(t, json.Unmarshal(response.body, &responseObject))
	assert.Equal(t, "completion-mock", responseObject["id"])

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, false)
	assertRewrittenExternalPayload(t, exchange.requestBody, capture.Body, externalOpenAIChatUpstreamModel, false)

	after := f.waitForMetricDelta(t, before, externalOpenAIChatModel, path, http.StatusOK, successfulExternalRequest, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 3)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assertSuccessfulExternalAccessLog(t, entry, externalOpenAIChatModel, path, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 3)
	assertPositiveExternalInputAccounting(t, before, after, entry)
}

// testOpenAICompletionsStreaming covers usage injection and SSE forwarding for
// the legacy Completions response shape.
func (f externalProviderFixture) testOpenAICompletionsStreaming(t *testing.T) {
	path := externalOpenAICompletionsPath
	before := f.readMetricSnapshot(t, externalOpenAIChatModel, path, http.StatusOK, successfulExternalRequest, testNamespace, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel)
	exchange := f.roundTrip(t, openAICompletionsRequest(externalOpenAIChatModel, `{
		"prompt":"Complete this sentence.",
		"stream":true,
		"stream_options":{"vendor_option":"preserve-me"},
		"max_tokens":16
	}`))
	response := exchange.response
	require.Equal(t, http.StatusOK, response.statusCode, "response: %s", response.body)
	assert.Contains(t, response.header.Get("Content-Type"), "text/event-stream")
	assert.Contains(t, string(response.body), `"object":"text_completion"`)
	assert.Contains(t, string(response.body), `data: [DONE]`)

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, false)
	assertRewrittenExternalPayload(t, exchange.requestBody, capture.Body, externalOpenAIChatUpstreamModel, true)

	after := f.waitForMetricDelta(t, before, externalOpenAIChatModel, path, http.StatusOK, successfulExternalRequest, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 3)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assertSuccessfulExternalAccessLog(t, entry, externalOpenAIChatModel, path, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 3)
	assertPositiveExternalInputAccounting(t, before, after, entry)
}

// testOpenAIResponsesNonStreaming exercises the Responses-specific JSON and usage shape.
func (f externalProviderFixture) testOpenAIResponsesNonStreaming(t *testing.T) {
	path := externalOpenAIResponsesPath
	before := f.readMetricSnapshot(t, externalResponsesModel, path, http.StatusOK, successfulExternalRequest, testNamespace, externalResponsesRouteName, externalResponsesProviderName, externalResponsesUpstreamModel)
	exchange := f.roundTrip(t, openAIResponsesRequest(externalResponsesModel, `{
		"input":"Return a short JSON response.",
		"stream":false
	}`))
	response := exchange.response
	require.Equal(t, http.StatusOK, response.statusCode, "response: %s", response.body)
	assert.Equal(t, "application/json", response.header.Get("Content-Type"))
	var responseObject map[string]any
	require.NoError(t, json.Unmarshal(response.body, &responseObject))
	assert.Equal(t, "resp-mock", responseObject["id"])

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, false)
	assertRewrittenExternalPayload(t, exchange.requestBody, capture.Body, externalResponsesUpstreamModel, false)

	after := f.waitForMetricDelta(t, before, externalResponsesModel, path, http.StatusOK, successfulExternalRequest, externalResponsesRouteName, externalResponsesProviderName, externalResponsesUpstreamModel, 4)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assertSuccessfulExternalAccessLog(t, entry, externalResponsesModel, path, externalResponsesRouteName, externalResponsesProviderName, externalResponsesUpstreamModel, 4)
	assertPositiveExternalInputAccounting(t, before, after, entry)
}

// testOpenAIResponsesStreaming verifies that Responses inputs unknown to Chat
// Completions survive forwarding and that the terminal event supplies usage.
func (f externalProviderFixture) testOpenAIResponsesStreaming(t *testing.T) {
	path := externalOpenAIResponsesPath
	before := f.readMetricSnapshot(t, externalResponsesModel, path, http.StatusOK, successfulExternalRequest, testNamespace, externalResponsesRouteName, externalResponsesProviderName, externalResponsesUpstreamModel)
	exchange := f.roundTrip(t, openAIResponsesRequest(externalResponsesModel, `{
		"stream":true,
		"input":[
			{"type":"message","role":"user","content":[
				{"type":"input_text","text":"Inspect the inputs."},
				{"type":"input_image","image_url":"data:image/png;base64,aW1hZ2U="},
				{"type":"input_file","file_id":"file-e2e"}
			]},
			{"type":"additional_tools","tools":[{"type":"custom","name":"shell"}]},
			{"type":"custom_tool_call","call_id":"custom-call","name":"shell","input":"echo ok"},
			{"type":"custom_tool_call_output","call_id":"custom-call","output":"ok"}
		],
		"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]
	}`))
	response := exchange.response
	require.Equal(t, http.StatusOK, response.statusCode, "response: %s", response.body)
	assert.Contains(t, response.header.Get("Content-Type"), "text/event-stream")
	assert.Contains(t, string(response.body), `"type":"response.completed"`)

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, false)
	assertRewrittenExternalPayload(t, exchange.requestBody, capture.Body, externalResponsesUpstreamModel, false)

	after := f.waitForMetricDelta(t, before, externalResponsesModel, path, http.StatusOK, successfulExternalRequest, externalResponsesRouteName, externalResponsesProviderName, externalResponsesUpstreamModel, 4)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assertSuccessfulExternalAccessLog(t, entry, externalResponsesModel, path, externalResponsesRouteName, externalResponsesProviderName, externalResponsesUpstreamModel, 4)
	assertPositiveExternalInputAccounting(t, before, after, entry)
}

// testAnthropicNonStreaming covers the default x-api-key authentication path and
// the Anthropic JSON response shape.
func (f externalProviderFixture) testAnthropicNonStreaming(t *testing.T) {
	path := externalAnthropicMessagesPath
	before := f.readMetricSnapshot(t, externalAnthropicModel, path, http.StatusOK, successfulExternalRequest, testNamespace, externalAnthropicRouteName, externalAnthropicProviderName, externalAnthropicUpstreamModel)
	exchange := f.roundTrip(t, anthropicMessagesRequest(externalAnthropicModel, `{
		"max_tokens":16,
		"stream":false,
		"messages":[{"role":"user","content":"Return a short JSON response."}]
	}`))
	response := exchange.response
	require.Equal(t, http.StatusOK, response.statusCode, "response: %s", response.body)
	assert.Equal(t, "application/json", response.header.Get("Content-Type"))
	var responseObject map[string]any
	require.NoError(t, json.Unmarshal(response.body, &responseObject))
	assert.Equal(t, "msg-mock", responseObject["id"])

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, true)
	assert.Equal(t, "APIKey", capture.AuthScheme)
	assertRewrittenExternalPayload(t, exchange.requestBody, capture.Body, externalAnthropicUpstreamModel, false)

	after := f.waitForMetricDelta(t, before, externalAnthropicModel, path, http.StatusOK, successfulExternalRequest, externalAnthropicRouteName, externalAnthropicProviderName, externalAnthropicUpstreamModel, 6)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assertSuccessfulExternalAccessLog(t, entry, externalAnthropicModel, path, externalAnthropicRouteName, externalAnthropicProviderName, externalAnthropicUpstreamModel, 6)
	assertPositiveExternalInputAccounting(t, before, after, entry)
}

// testAnthropicBearerGateway proves that an Anthropic-compatible gateway can
// override authentication while retaining client-supplied beta headers.
func (f externalProviderFixture) testAnthropicBearerGateway(t *testing.T) {
	path := externalAnthropicMessagesPath
	beta := "context-1m-2025-08-07"

	before := f.readMetricSnapshot(t, externalAnthropicBearerModel, path, http.StatusOK, successfulExternalRequest, testNamespace, externalAnthropicBearerRouteName, externalAnthropicBearerProviderName, externalAnthropicUpstreamModel)
	request := anthropicMessagesRequest(externalAnthropicBearerModel, `{
		"max_tokens":16,
		"stream":false,
		"messages":[{"role":"user","content":"Reply with OK."}]
	}`)
	request.headers = http.Header{"Anthropic-Beta": []string{beta}}
	exchange := f.roundTrip(t, request)
	response := exchange.response
	require.Equal(t, http.StatusOK, response.statusCode, "response: %s", response.body)

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, true)
	assert.Equal(t, "Bearer", capture.AuthScheme)
	assert.Equal(t, beta, capture.AnthropicBeta)
	assertRewrittenExternalPayload(t, exchange.requestBody, capture.Body, externalAnthropicUpstreamModel, false)

	after := f.waitForMetricDelta(t, before, externalAnthropicBearerModel, path, http.StatusOK, successfulExternalRequest, externalAnthropicBearerRouteName, externalAnthropicBearerProviderName, externalAnthropicUpstreamModel, 6)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assertSuccessfulExternalAccessLog(t, entry, externalAnthropicBearerModel, path, externalAnthropicBearerRouteName, externalAnthropicBearerProviderName, externalAnthropicUpstreamModel, 6)
	assertPositiveExternalInputAccounting(t, before, after, entry)
}

// testAnthropicStreaming covers tool, image, and document blocks together with
// the complete Anthropic Messages SSE lifecycle.
func (f externalProviderFixture) testAnthropicStreaming(t *testing.T) {
	path := externalAnthropicMessagesPath
	before := f.readMetricSnapshot(t, externalAnthropicModel, path, http.StatusOK, successfulExternalRequest, testNamespace, externalAnthropicRouteName, externalAnthropicProviderName, externalAnthropicUpstreamModel)
	exchange := f.roundTrip(t, anthropicMessagesRequest(externalAnthropicModel, `{
		"max_tokens":32,
		"stream":true,
		"tools":[{"name":"weather","description":"Get weather","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],
		"messages":[
			{"role":"user","content":[
				{"type":"text","text":"Inspect the attachments."},
				{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aW1hZ2U="}},
				{"type":"document","source":{"type":"base64","media_type":"application/pdf","data":"ZmlsZQ=="}}
			]},
			{"role":"assistant","content":[{"type":"tool_use","id":"tool-weather","name":"weather","input":{"city":"Shanghai"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-weather","content":[{"type":"text","text":"sunny"}]}]}
		]
	}`))
	response := exchange.response
	require.Equal(t, http.StatusOK, response.statusCode, "response: %s", response.body)
	assert.Contains(t, response.header.Get("Content-Type"), "text/event-stream")
	assert.Contains(t, string(response.body), `"type":"message_stop"`)

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, true)
	assertRewrittenExternalPayload(t, exchange.requestBody, capture.Body, externalAnthropicUpstreamModel, false)

	after := f.waitForMetricDelta(t, before, externalAnthropicModel, path, http.StatusOK, successfulExternalRequest, externalAnthropicRouteName, externalAnthropicProviderName, externalAnthropicUpstreamModel, 6)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assertSuccessfulExternalAccessLog(t, entry, externalAnthropicModel, path, externalAnthropicRouteName, externalAnthropicProviderName, externalAnthropicUpstreamModel, 6)
	assertPositiveExternalInputAccounting(t, before, after, entry)
}

// testNonTextInputAccounting fixes the accounting boundary: non-text input is
// forwarded but excluded from the Router's local input-token estimate.
func (f externalProviderFixture) testNonTextInputAccounting(t *testing.T) {
	path := externalOpenAIChatPath
	before := f.readMetricSnapshot(t, externalOpenAIChatModel, path, http.StatusOK, successfulExternalRequest, testNamespace, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel)
	exchange := f.roundTrip(t, openAIChatRequest(externalOpenAIChatModel, `{
		"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,aW1hZ2U="}}]}],
		"stream":false,
		"max_tokens":16
	}`))
	response := exchange.response
	require.Equal(t, http.StatusOK, response.statusCode, "response: %s", response.body)
	var providerResponse struct {
		Usage struct {
			PromptTokens int `json:"prompt_tokens"`
		} `json:"usage"`
	}
	require.NoError(t, json.Unmarshal(response.body, &providerResponse))
	assert.Positive(t, providerResponse.Usage.PromptTokens, "mock provider usage must prove that local input accounting ignores provider input usage")

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, false)
	assertRewrittenExternalPayload(t, exchange.requestBody, capture.Body, externalOpenAIChatUpstreamModel, false)
	assert.Contains(t, string(capture.Body), `"type":"image_url"`)

	after := f.waitForMetricDelta(t, before, externalOpenAIChatModel, path, http.StatusOK, successfulExternalRequest, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 5)
	assert.Equal(t, float64(0), after.inputTokens-before.inputTokens)
	assert.Equal(t, float64(5), after.outputTokens-before.outputTokens)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assertSuccessfulExternalAccessLog(t, entry, externalOpenAIChatModel, path, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 5)
	assert.Equal(t, 0, entry.InputTokens)
}

// testUpstream429 verifies that a provider rejection reaches the client unchanged
// and is attributed to the upstream without recording output tokens.
func (f externalProviderFixture) testUpstream429(t *testing.T) {
	path := externalOpenAIChatPath
	before := f.readMetricSnapshot(t, externalOpenAIChatModel, path, http.StatusTooManyRequests, "upstream_response", testNamespace, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel)
	request := openAIChatRequest(externalOpenAIChatModel, `{"messages":[{"role":"user","content":"rate limit me"}],"stream":false}`)
	request.query = url.Values{"mock_status": []string{"429"}}
	exchange := f.roundTrip(t, request)
	response := exchange.response
	require.Equal(t, http.StatusTooManyRequests, response.statusCode, "response: %s", response.body)
	assert.Contains(t, string(response.body), `"type":"rate_limit_error"`)

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, false)
	assert.Equal(t, "mock_status=429", capture.RawQuery)
	assertRewrittenExternalPayload(t, exchange.requestBody, capture.Body, externalOpenAIChatUpstreamModel, false)

	after := f.waitForMetricDelta(t, before, externalOpenAIChatModel, path, http.StatusTooManyRequests, "upstream_response", externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 0)
	assert.Equal(t, float64(0), after.outputTokens-before.outputTokens)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assert.Equal(t, http.StatusTooManyRequests, entry.StatusCode)
	assert.Equal(t, http.StatusTooManyRequests, entry.UpstreamStatusCode)
	assert.Equal(t, 1, entry.UpstreamAttempts)
	assert.Equal(t, "external_provider", entry.BackendType)
	assert.Equal(t, testNamespace+"/"+externalOpenAIChatRouteName, entry.ModelRoute)
	assert.Equal(t, testNamespace+"/"+externalOpenAIChatProviderName, entry.BackendName)
	assert.Equal(t, externalOpenAIChatUpstreamModel, entry.UpstreamModel)
	assert.Equal(t, "upstream", entry.ErrorOrigin)
	assert.Equal(t, 0, entry.OutputTokens)
	require.NotNil(t, entry.Error)
	assert.Equal(t, "upstream_response", entry.Error.Type)
}

// testActiveGaugeLifecycle holds an upstream request open long enough to observe
// both active gauges, then verifies that they return to their baselines.
func (f externalProviderFixture) testActiveGaugeLifecycle(t *testing.T) {
	request := openAIChatRequest(externalOpenAIChatModel, `{"messages":[{"role":"user","content":"wait"}],"stream":false}`)
	request.query = url.Values{"mock_delay_ms": []string{"1500"}}
	exchange := f.prepareRequest(t, request)
	path := request.path
	downstreamBaseline, upstreamBaseline := f.readActiveGauges(t, externalOpenAIChatModel, testNamespace, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel)

	type requestResult struct {
		response externalHTTPResponse
		err      error
	}
	resultCh := make(chan requestResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		response, err := exchange.send(ctx, f.routerURL)
		resultCh <- requestResult{response: response, err: err}
	}()

	require.Eventually(t, func() bool {
		downstream, upstream, err := tryReadExternalActiveGauges(f.metricsURL, externalOpenAIChatModel, testNamespace, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel)
		if err != nil {
			return false
		}
		return downstream >= downstreamBaseline+1 && upstream >= upstreamBaseline+1
	}, 3*time.Second, 100*time.Millisecond, "active downstream and upstream gauges did not rise during the delayed request")

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		require.Equal(t, http.StatusOK, result.response.statusCode, "response: %s", result.response.body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delayed external provider request")
	}

	require.Eventually(t, func() bool {
		downstream, upstream, err := tryReadExternalActiveGauges(f.metricsURL, externalOpenAIChatModel, testNamespace, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel)
		if err != nil {
			return false
		}
		return downstream == downstreamBaseline && upstream == upstreamBaseline
	}, 3*time.Second, 100*time.Millisecond, "active downstream and upstream gauges did not return to baseline")

	capture := exchange.capture(t)
	assertExternalCapture(t, capture, path, false)
	assert.Equal(t, "mock_delay_ms=1500", capture.RawQuery)
	entry := waitForExternalAccessLog(t, testCtx.KubeClient, kthenaNamespace, exchange.requestID)
	assertSuccessfulExternalAccessLog(t, entry, externalOpenAIChatModel, path, externalOpenAIChatRouteName, externalOpenAIChatProviderName, externalOpenAIChatUpstreamModel, 5)
	assert.Positive(t, entry.InputTokens)
	assert.GreaterOrEqual(t, entry.DurationUpstreamProcessing, int64(1000))
	assert.GreaterOrEqual(t, entry.DurationTotal, entry.DurationUpstreamProcessing)
}

// assertExternalCapture checks normalized request metadata and the protocol-specific
// authentication and version headers added by the Router.
func assertExternalCapture(t *testing.T, capture mockCapture, path string, anthropic bool) {
	t.Helper()
	assert.Equal(t, http.MethodPost, capture.Method)
	assert.Equal(t, path, capture.Path)
	assert.Equal(t, "application/json", capture.ContentType)
	assert.True(t, capture.Authorized)
	if anthropic {
		assert.Equal(t, "2023-06-01", capture.AnthropicVersion)
	} else {
		assert.Empty(t, capture.AnthropicVersion)
	}
}

// assertPositiveExternalInputAccounting keeps the metric and access-log views of
// the Router's local input estimate in sync.
func assertPositiveExternalInputAccounting(t *testing.T, before, after externalMetricSnapshot, entry accesslog.AccessLogEntry) {
	t.Helper()
	delta := after.inputTokens - before.inputTokens
	require.Positive(t, delta, "text input must increment the locally estimated input-token metric")
	assert.Equal(t, delta, float64(entry.InputTokens), "metric and access log must use the same local input-token estimate")
}

// assertRewrittenExternalPayload permits only the documented model rewrite and,
// when requested, the OpenAI stream usage option added by the Router.
func assertRewrittenExternalPayload(t *testing.T, original, captured []byte, upstreamModel string, expectStreamUsageInjection bool) {
	t.Helper()
	var want, got map[string]any
	require.NoError(t, json.Unmarshal(original, &want))
	require.NoError(t, json.Unmarshal(captured, &got))
	assert.Equal(t, upstreamModel, got["model"])
	delete(want, "model")
	delete(got, "model")
	if expectStreamUsageInjection {
		wantOptions, _ := want["stream_options"].(map[string]any)
		gotOptions, ok := got["stream_options"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, true, gotOptions["include_usage"])
		delete(gotOptions, "include_usage")
		assert.Equal(t, wantOptions, gotOptions)
		delete(want, "stream_options")
		delete(got, "stream_options")
	} else {
		assert.NotContains(t, got, "include_usage")
		assert.NotContains(t, got, "stream_options")
	}
	assert.Equal(t, want, got, "opaque non-model payload fields must be preserved")
}

// waitForMetricDelta waits for every metric family affected by one request so a
// later scenario cannot accidentally satisfy an earlier assertion.
func (f externalProviderFixture) waitForMetricDelta(t *testing.T, before externalMetricSnapshot, model, path string, statusCode int, errorType, route, provider, upstreamModel string, outputTokens float64) externalMetricSnapshot {
	t.Helper()
	var after externalMetricSnapshot
	require.Eventually(t, func() bool {
		var err error
		after, err = tryReadExternalMetricSnapshot(f.metricsURL, model, path, statusCode, errorType, testNamespace, route, provider, upstreamModel)
		if err != nil {
			return false
		}
		return after.requests-before.requests == 1 &&
			after.durationCount-before.durationCount == 1 &&
			after.outputTokens-before.outputTokens == outputTokens
	}, 10*time.Second, 250*time.Millisecond, "external provider metrics did not record one request and %.0f output tokens", outputTokens)
	return after
}

// assertSuccessfulExternalAccessLog checks the destination identity and provider
// attempt data emitted for a successful request.
func assertSuccessfulExternalAccessLog(t *testing.T, entry accesslog.AccessLogEntry, model, path, route, provider, upstreamModel string, outputTokens int) {
	t.Helper()
	assert.Equal(t, http.MethodPost, entry.Method)
	assert.Equal(t, path, entry.Path)
	assert.Equal(t, http.StatusOK, entry.StatusCode)
	assert.Equal(t, model, entry.ModelName)
	assert.Equal(t, testNamespace+"/"+route, entry.ModelRoute)
	assert.Equal(t, "external_provider", entry.BackendType)
	assert.Equal(t, testNamespace+"/"+provider, entry.BackendName)
	assert.Equal(t, upstreamModel, entry.UpstreamModel)
	assert.Equal(t, http.StatusOK, entry.UpstreamStatusCode)
	assert.Equal(t, 1, entry.UpstreamAttempts)
	assert.Equal(t, outputTokens, entry.OutputTokens)
	assert.Empty(t, entry.ModelServer)
	assert.Empty(t, entry.SelectedPod)
	assert.Empty(t, entry.ErrorOrigin)
	assert.Nil(t, entry.Error)
	assert.GreaterOrEqual(t, entry.DurationTotal, int64(0))
	assert.GreaterOrEqual(t, entry.DurationUpstreamProcessing, int64(0))
}
