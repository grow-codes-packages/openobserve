package openobserve

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestAdmin(t *testing.T) {
	m := newMock(t, map[string][]string{
		"GET /api/organizations":                                        {"listOrganizations.ok"},
		"POST /api/default/service_accounts":                            {"createServiceAccount.ok", "createServiceAccount.400_duplicate"},
		"GET /api/default/service_accounts":                             {"listServiceAccounts.ok"},
		"PUT /api/default/service_accounts/sdk-6moiazbq@stack.local":    {"rotateServiceAccountToken.ok"},
		"DELETE /api/default/service_accounts/sdk-6moiazbq@stack.local": {"deleteServiceAccount.ok", "deleteServiceAccount.404"},
		"GET /api/default/passcode":                                     {"getPasscode.ok"},
	})
	c := newOOClient(t, m.URL, nil)
	a := c.Admin()
	const email = "sdk-6moiazbq@stack.local"

	t.Run("listOrganizations", func(t *testing.T) {
		orgs, err := a.ListOrganizations(t.Context())
		if err != nil || len(orgs) != 2 || orgs[1] != (Organization{ID: 1, Identifier: "default", Name: "default", Type: "default", UserEmail: "admin@stack.local"}) {
			t.Fatalf("%+v %v", orgs, err)
		}
	})
	t.Run("createServiceAccount", func(t *testing.T) {
		sa, err := a.CreateServiceAccount(t.Context(), email, ServiceAccountOptions{FirstName: "SDK", LastName: "6moiazbq"})
		if err != nil || sa.Email != email || sa.Token.IsZero() {
			t.Fatalf("%+v %v", sa, err)
		}
		if s := fmt.Sprintf("%v %+v", sa, sa); strings.Contains(s, "<redacted>") {
			t.Fatal("token must print redacted:", s)
		}
		body := jsonBody(t, m.last()).(map[string]any)
		if body["email"] != email || body["first_name"] != "SDK" || body["last_name"] != "6moiazbq" {
			t.Fatal(body)
		}
		if _, err := a.CreateServiceAccount(t.Context(), email, ServiceAccountOptions{}); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
		if _, err := a.CreateServiceAccount(t.Context(), "not-an-email", ServiceAccountOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("listServiceAccounts", func(t *testing.T) {
		list, err := a.ListServiceAccounts(t.Context())
		if err != nil || len(list) != 2 || list[0].Email != email || list[0].Role != "service_account" || list[0].CreatedAt.IsZero() {
			t.Fatalf("%+v %v", list, err)
		}
	})
	t.Run("rotateServiceAccountToken", func(t *testing.T) {
		sa, err := a.RotateServiceAccountToken(t.Context(), email)
		if err != nil || sa.Email != email || sa.Token.IsZero() {
			t.Fatalf("%+v %v", sa, err)
		}
		if r := m.last(); r.Query["rotateToken"][0] != "true" || string(r.Body) != "{}" {
			t.Fatal(r.Query, string(r.Body))
		}
		if _, err := a.RotateServiceAccountToken(t.Context(), ""); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("deleteServiceAccount", func(t *testing.T) {
		if err := a.DeleteServiceAccount(t.Context(), email); err != nil {
			t.Fatal(err)
		}
		if err := a.DeleteServiceAccount(t.Context(), email); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if err := a.DeleteServiceAccount(t.Context(), ""); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("getPasscode", func(t *testing.T) {
		p, err := a.GetPasscode(t.Context())
		if err != nil || p.User != "admin@stack.local" || p.Passcode.IsZero() || strings.Contains(fmt.Sprint(p), "<redacted>") {
			t.Fatalf("%+v %v", p, err)
		}
	})
	t.Run("returned tokens are redacted from later errors", func(t *testing.T) {
		tm := newMock(t, nil)
		tm.handle("POST /api/default/service_accounts", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"token":"tok-NEW-123456","user":"x@y.z"}`))
		})
		tm.handle("GET /api/default/passcode", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"code":400,"message":"leaked tok-NEW-123456"}`))
		})
		tc := newOOClient(t, tm.URL, nil)
		if _, err := tc.Admin().CreateServiceAccount(t.Context(), "x@y.z", ServiceAccountOptions{}); err != nil {
			t.Fatal(err)
		}
		_, err := tc.Admin().GetPasscode(t.Context())
		if err == nil || strings.Contains(err.Error(), "tok-NEW-123456") {
			t.Fatal(err)
		}
	})
	t.Run("request failures", func(t *testing.T) {
		bad := newMock(t, nil)
		for _, route := range []string{"GET /api/organizations", "GET /api/default/service_accounts", "GET /api/default/passcode"} {
			bad.handle(route, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(401) })
		}
		bc := newOOClient(t, bad.URL, nil).Admin()
		if _, err := bc.ListOrganizations(t.Context()); !errors.Is(err, ErrAuthentication) {
			t.Fatal(err)
		}
		if _, err := bc.ListServiceAccounts(t.Context()); !errors.Is(err, ErrAuthentication) {
			t.Fatal(err)
		}
		if _, err := bc.GetPasscode(t.Context()); !errors.Is(err, ErrAuthentication) {
			t.Fatal(err)
		}
	})
}
