package openobserve

import (
	"context"
	"net/http"
	"net/mail"
	"net/url"
	"time"
)

// Admin holds organization and service-account helpers (Client.Admin). The
// open-source OpenObserve build has no RBAC: every service account can ingest,
// query and administer; the passcode is the only ingest-only credential.
type Admin struct{ c *Client }

// Admin returns the admin helpers.
func (c *Client) Admin() *Admin { return &Admin{c: c} }

// Organization is an OpenObserve organization.
type Organization struct {
	ID         int64  `json:"id"`
	Identifier string `json:"identifier"`
	Name       string `json:"name"`
	// Type is "default" or "custom".
	Type      string `json:"type"`
	UserEmail string `json:"userEmail"`
}

// ListOrganizations lists the organizations visible to the caller (GET
// /api/organizations), including the internal "_meta" organization.
func (a *Admin) ListOrganizations(ctx context.Context) ([]Organization, error) {
	var body struct {
		Data []struct {
			ID         int64  `json:"id"`
			Identifier string `json:"identifier"`
			Name       string `json:"name"`
			Type       string `json:"type"`
			UserEmail  string `json:"user_email"`
		} `json:"data"`
	}
	if err := a.c.getJSON(ctx, "admin.listOrganizations", &request{Method: http.MethodGet, Path: "/api/organizations"}, &body); err != nil {
		return nil, err
	}
	out := make([]Organization, 0, len(body.Data))
	for _, o := range body.Data {
		out = append(out, Organization{ID: o.ID, Identifier: o.Identifier, Name: o.Name, Type: o.Type, UserEmail: o.UserEmail})
	}
	return out, nil
}

// ServiceAccountOptions are the optional names of a new service account.
type ServiceAccountOptions struct {
	FirstName string
	LastName  string
}

// ServiceAccountToken is a service account with its usable token. The token is
// returned only by CreateServiceAccount and RotateServiceAccountToken; use it
// as Config{User: Email, Token: Token}.
type ServiceAccountToken struct {
	Email string `json:"email"`
	Token Secret `json:"token"`
}

// ServiceAccount is a listed service account (its token is masked by the server
// and not returned).
type ServiceAccount struct {
	Email     string    `json:"email"`
	FirstName string    `json:"firstName"`
	LastName  string    `json:"lastName"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"createdAt"`
}

func (a *Admin) tokenResult(ctx context.Context, op string, req *request) (ServiceAccountToken, error) {
	var body struct {
		User  string `json:"user"`
		Token string `json:"token"`
	}
	if err := a.c.getJSON(ctx, op, req, &body); err != nil {
		return ServiceAccountToken{}, err
	}
	if body.Token != "" {
		a.c.transport.addSecret(body.Token)
	}
	return ServiceAccountToken{Email: body.User, Token: NewSecret(body.Token)}, nil
}

// CreateServiceAccount creates a service account and returns its token (POST
// /api/{org}/service_accounts). CONFLICT when it already exists, VALIDATION for
// an invalid e-mail.
func (a *Admin) CreateServiceAccount(ctx context.Context, email string, opts ServiceAccountOptions) (ServiceAccountToken, error) {
	const op = "admin.createServiceAccount"
	if _, err := mail.ParseAddress(email); err != nil {
		return ServiceAccountToken{}, ValidationError(op, "email is not a valid address")
	}
	return a.tokenResult(ctx, op, &request{
		Method: http.MethodPost, Path: a.c.orgPath("/service_accounts"),
		JSON: map[string]string{"email": email, "first_name": opts.FirstName, "last_name": opts.LastName},
	})
}

// ListServiceAccounts lists the organization's service accounts (GET
// /api/{org}/service_accounts).
func (a *Admin) ListServiceAccounts(ctx context.Context) ([]ServiceAccount, error) {
	var body struct {
		Data []struct {
			Email     string `json:"email"`
			FirstName string `json:"first_name"`
			LastName  string `json:"last_name"`
			Role      string `json:"role"`
			CreatedAt any    `json:"created_at"`
		} `json:"data"`
	}
	if err := a.c.getJSON(ctx, "admin.listServiceAccounts", &request{Method: http.MethodGet, Path: a.c.orgPath("/service_accounts")}, &body); err != nil {
		return nil, err
	}
	out := make([]ServiceAccount, 0, len(body.Data))
	for _, s := range body.Data {
		sa := ServiceAccount{Email: s.Email, FirstName: s.FirstName, LastName: s.LastName, Role: s.Role}
		if us, ok := toInt64(s.CreatedAt); ok && us > 0 {
			sa.CreatedAt = fromMicros(us)
		}
		out = append(out, sa)
	}
	return out, nil
}

// RotateServiceAccountToken issues a new token; the old one stops working at
// once (PUT /api/{org}/service_accounts/{email}?rotateToken=true).
func (a *Admin) RotateServiceAccountToken(ctx context.Context, email string) (ServiceAccountToken, error) {
	const op = "admin.rotateServiceAccountToken"
	if email == "" {
		return ServiceAccountToken{}, ValidationError(op, "email is required")
	}
	return a.tokenResult(ctx, op, &request{
		Method: http.MethodPut, Path: a.c.orgPath("/service_accounts/" + email),
		Query: url.Values{"rotateToken": {"true"}}, JSON: map[string]string{},
	})
}

// DeleteServiceAccount removes a service account (DELETE
// /api/{org}/service_accounts/{email}); NOT_FOUND when it does not exist.
func (a *Admin) DeleteServiceAccount(ctx context.Context, email string) error {
	const op = "admin.deleteServiceAccount"
	if email == "" {
		return ValidationError(op, "email is required")
	}
	_, err := a.c.do(ctx, &request{Method: http.MethodDelete, Path: a.c.orgPath("/service_accounts/" + email), Operation: op})
	return err
}

// Passcode is the caller's ingest-only credential: Basic User:Passcode may
// ingest but not search or administer.
type Passcode struct {
	User     string `json:"user"`
	Passcode Secret `json:"passcode"`
}

// GetPasscode returns the caller's ingestion passcode (GET /api/{org}/passcode).
func (a *Admin) GetPasscode(ctx context.Context) (Passcode, error) {
	var body struct {
		Data struct {
			User     string `json:"user"`
			Passcode string `json:"passcode"`
		} `json:"data"`
	}
	if err := a.c.getJSON(ctx, "admin.getPasscode", &request{Method: http.MethodGet, Path: a.c.orgPath("/passcode")}, &body); err != nil {
		return Passcode{}, err
	}
	if body.Data.Passcode != "" {
		a.c.transport.addSecret(body.Data.Passcode)
	}
	return Passcode{User: body.Data.User, Passcode: NewSecret(body.Data.Passcode)}, nil
}
