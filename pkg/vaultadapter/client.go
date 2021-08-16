package vaultadapter

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/vault/api"
)

// ClientConfig contains all attributes needed to estabilish a connection with Vault.
type ClientConfig struct {
	URL      string
	RoleID   string
	SecretID string
}

// Client helps the Adapter to estabilish a connection with Vault.
type Client struct {
	vaultClient *api.Client
	roleID      string
	secretID    string
}

// NewClient will create a new client from the vault api and will try to log in.
func NewClient(c ClientConfig) (Client, error) {
	config := &api.Config{
		Address:    c.URL,
		MaxRetries: 5,
		Timeout:    time.Second * 30,
		Error:      nil,
	}

	client, err := api.NewClient(config)
	if err != nil {
		return Client{}, err
	}

	err = login(client, c.RoleID, c.SecretID)
	if err != nil {
		return Client{}, err
	}

	return Client{
		vaultClient: client,
		roleID:      c.RoleID,
		secretID:    c.SecretID,
	}, nil
}

// ReadKV will try to get the variables of the given path on Vault, and if receive
// error will try to log in and read the variables again.
func (c *Client) ReadKV(path string) (map[string]string, error) {
	path = strings.TrimSpace(path)

	data, err := read(c.vaultClient, path)
	if err == nil {
		return data, nil
	}

	err = login(c.vaultClient, c.roleID, c.secretID)
	if err != nil {
		return nil, err
	}

	return read(c.vaultClient, path)
}

func read(c *api.Client, path string) (map[string]string, error) {
	secrets := make(map[string]string)

	secret, err := c.Logical().Read(path)
	if err != nil {
		return nil, err
	}

	if secret == nil {
		return nil, errors.New("nil read response")
	}

	for k, v := range secret.Data["data"].(map[string]interface{}) {
		secrets[k] = v.(string)
	}

	return secrets, nil
}

func login(c *api.Client, r string, s string) error {
	data := map[string]interface{}{
		"role_id":   r,
		"secret_id": s,
	}

	resp, err := c.Logical().Write("auth/approle/login", data)
	if err != nil {
		return err
	}

	if resp.Auth == nil {
		return fmt.Errorf("no auth info returned")
	}

	c.SetToken(resp.Auth.ClientToken)

	return nil
}
