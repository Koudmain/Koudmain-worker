package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"koudmain-worker/internal/model"
)

// InternalSecretHeader est le header d'authentification des endpoints
// internes de NestJS.
const InternalSecretHeader = "X-Internal-Secret" //nolint:gosec // G101: nom de header, pas un secret

// maxErrorBodyBytes limite la taille du corps de réponse recopié dans les
// erreurs pour garder des logs lisibles.
const maxErrorBodyBytes = 512

// BackendClient appelle les endpoints internes de NestJS. Le worker n'accède
// jamais directement à Postgres : la lecture des device tokens, l'audit des
// envois et la suppression des tokens invalides passent par ce client.
type BackendClient struct {
	baseURL    string
	secret     string
	httpClient *http.Client
}

// NewBackendClient crée un client vers `baseURL` (ex: http://backend:3000)
// authentifié par `secret` (header X-Internal-Secret). Chaque requête est
// bornée par `timeout`.
func NewBackendClient(baseURL, secret string, timeout time.Duration) *BackendClient {
	return &BackendClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		secret:     secret,
		httpClient: &http.Client{Timeout: timeout},
	}
}

// GetDeviceTokens retourne les device tokens des utilisateurs `userIDs`
// (GET /internal/device-tokens?userIds=1,2,3).
func (c *BackendClient) GetDeviceTokens(ctx context.Context, userIDs []int) ([]model.DeviceToken, error) {
	ids := make([]string, 0, len(userIDs))
	for _, id := range userIDs {
		ids = append(ids, strconv.Itoa(id))
	}
	endpoint := c.baseURL + "/internal/device-tokens?userIds=" + strings.Join(ids, ",")

	resp, err := c.do(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := expectStatus(resp, http.StatusOK); err != nil {
		return nil, err
	}

	var tokens []model.DeviceToken
	if err := json.NewDecoder(resp.Body).Decode(&tokens); err != nil {
		return nil, fmt.Errorf("réponse device-tokens invalide: %w", err)
	}
	return tokens, nil
}

// RecordDelivery enregistre l'audit d'un envoi push
// (POST /internal/notification-deliveries).
func (c *BackendClient) RecordDelivery(ctx context.Context, delivery model.NotificationDelivery) error {
	body, err := json.Marshal(delivery)
	if err != nil {
		return fmt.Errorf("sérialisation de l'audit impossible: %w", err)
	}

	resp, err := c.do(ctx, http.MethodPost, c.baseURL+"/internal/notification-deliveries", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return expectStatus(resp, http.StatusCreated, http.StatusOK)
}

// DeleteDeviceToken supprime un device token invalide
// (DELETE /internal/device-tokens/:token). Un 404 est considéré comme un
// succès : le token n'existe déjà plus.
func (c *BackendClient) DeleteDeviceToken(ctx context.Context, token string) error {
	endpoint := c.baseURL + "/internal/device-tokens/" + url.PathEscape(token)

	resp, err := c.do(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return expectStatus(resp, http.StatusNoContent, http.StatusOK, http.StatusNotFound)
}

func (c *BackendClient) do(ctx context.Context, method, endpoint string, body []byte) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, fmt.Errorf("construction de la requête %s %s: %w", method, endpoint, err)
	}
	req.Header.Set(InternalSecretHeader, c.secret)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("appel %s %s: %w", method, req.URL.Path, err)
	}
	return resp, nil
}

// expectStatus retourne une erreur si le status de `resp` n'est pas dans
// `accepted`. Le corps (tronqué) est inclus dans l'erreur.
func expectStatus(resp *http.Response, accepted ...int) error {
	for _, status := range accepted {
		if resp.StatusCode == status {
			return nil
		}
	}
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	return fmt.Errorf("%s %s: status inattendu %d: %s",
		resp.Request.Method, resp.Request.URL.Path, resp.StatusCode, strings.TrimSpace(string(snippet)))
}
