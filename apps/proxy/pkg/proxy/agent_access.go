// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package proxy

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	common_errors "github.com/daytonaio/common-go/pkg/errors"
	"github.com/daytonaio/common-go/pkg/utils"
	apiclient "github.com/daytonaio/daytona/libs/api-client-go"
)

// SSH_ACCESS_TOKEN_QUERY_PARAM carries the SSH access token on WebSocket
// handshakes, where clients cannot set an Authorization header.
const SSH_ACCESS_TOKEN_QUERY_PARAM = "token"

// Agent access paths accept SSH access tokens (the same tokens the SSH
// gateway issues) as an alternative credential, giving agents SSH-equivalent
// sandbox access over plain HTTPS.
var agentAccessPaths = map[string]bool{
	"/process/exec/connect": true,
	"/mcp":                  true,
}

// isAgentAccessPath reports whether the toolbox target path is one of the
// agent-access endpoints that accept SSH access tokens.
func isAgentAccessPath(targetPath string) bool {
	return agentAccessPaths[strings.TrimSuffix(targetPath, "/")]
}

// getSshAccessTokenValid validates an SSH access token against the API.
// Unlike the preview-token validators, the result is intentionally NOT
// cached: revoking a token must block new connections immediately.
func (p *Proxy) getSshAccessTokenValid(ctx context.Context, sandboxId string, token string) (*bool, error) {
	isValid := false
	err := utils.RetryWithExponentialBackoff(ctx, "getSshAccessTokenValid", proxyMaxRetries, proxyBaseDelay, proxyMaxDelay, func() error {
		validation, resp, err := p.apiclient.SandboxAPI.ValidateSshAccess(context.Background()).Token(token).Execute()
		if resp != nil && resp.StatusCode == http.StatusOK {
			isValid = validation != nil && validation.Valid && validation.SandboxId == sandboxId
			return nil
		}
		openapiErr := common_errors.ConvertOpenAPIError(err)

		if openapiErr != nil {
			if resp != nil && resp.StatusCode >= 400 && resp.StatusCode < 500 &&
				resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusTooManyRequests {
				isValid = false
				return nil
			}
			if !common_errors.IsRetryableOpenAPIError(openapiErr) {
				return &utils.NonRetryableError{Err: openapiErr}
			}
			return openapiErr
		}
		isValid = false
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &isValid, nil
}

// ensureSandboxStarted mirrors the SSH gateway: connecting to a sandbox that
// is not started fails fast with an explicit state message instead of an
// opaque upstream error.
func (p *Proxy) ensureSandboxStarted(ctx context.Context, sandboxId string) error {
	sandbox, _, err := p.apiclient.SandboxAPI.GetSandbox(ctx, sandboxId).Execute()
	if err != nil {
		return common_errors.NewBadRequestError(fmt.Errorf("failed to verify sandbox state: %w", err))
	}

	if sandbox.State == nil || *sandbox.State != apiclient.SANDBOXSTATE_STARTED {
		state := "unknown"
		if sandbox.State != nil {
			state = string(*sandbox.State)
		}
		return common_errors.NewBadRequestError(fmt.Errorf("sandbox is not started (state: %s). Please start the sandbox before attempting to connect", state))
	}

	return nil
}
