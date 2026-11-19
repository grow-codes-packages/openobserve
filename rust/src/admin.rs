// Service-owned. Admin helpers (API.md §6): organizations, service accounts, passcode.
//! Admin helpers: organizations, service accounts and the ingest-only passcode.

use std::time::SystemTime;

use serde_json::{json, Value};

use crate::core::{Error, Method, Result, Secret};
use crate::http::{field, int, items, micros_to_system_time, send_json, text};
use crate::OpenObserveClient;

/// An organization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Organization {
    /// Numeric id.
    pub id: i64,
    /// Identifier used in URLs (`default`, …).
    pub identifier: String,
    /// Display name.
    pub name: String,
    /// `default` or `custom`.
    pub org_type: String,
    /// E-mail of the owner, when reported.
    pub user_email: Option<String>,
}

/// A service account (the server only lists masked tokens, so none is returned).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceAccount {
    /// Account e-mail (the Basic-auth user).
    pub email: String,
    /// First name.
    pub first_name: String,
    /// Last name.
    pub last_name: String,
    /// Role (`service_account`).
    pub role: String,
    /// Creation time.
    pub created_at: Option<SystemTime>,
}

/// A service-account credential: use `user` + `token` as `OPENOBSERVE_USER` /
/// `OPENOBSERVE_TOKEN`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceAccountToken {
    /// The account e-mail.
    pub user: String,
    /// The usable token (only returned on create and rotate).
    pub token: Secret,
}

/// An ingest-only credential (Basic `user:passcode` can ingest but not query).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Passcode {
    /// The user the passcode belongs to.
    pub user: String,
    /// The passcode.
    pub passcode: Secret,
}

/// Options of [`OpenObserveAdmin::create_service_account`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateServiceAccountOptions {
    /// First name.
    pub first_name: Option<String>,
    /// Last name.
    pub last_name: Option<String>,
}

/// Admin helpers: `client.admin()`. OSS has no RBAC; every service account can administer.
#[derive(Debug, Clone)]
pub struct OpenObserveAdmin {
    client: OpenObserveClient,
}

impl OpenObserveAdmin {
    pub(crate) fn new(client: OpenObserveClient) -> Self {
        OpenObserveAdmin { client }
    }

    /// Organizations visible to the user (`GET /api/organizations`, includes `_meta`).
    pub async fn list_organizations(&self) -> Result<Vec<Organization>> {
        let op = "admin.listOrganizations";
        let body = send_json(
            self.client
                .request(Method::GET, "/api/organizations")
                .operation(op),
            op,
        )
        .await?;
        Ok(items(field(&body, "data"))
            .iter()
            .map(|o| Organization {
                id: int(field(o, "id")),
                identifier: text(field(o, "identifier")).unwrap_or_default(),
                name: text(field(o, "name")).unwrap_or_default(),
                org_type: text(field(o, "type")).unwrap_or_default(),
                user_email: text(field(o, "user_email")),
            })
            .collect())
    }

    /// Creates a service account and returns its token (the only time the server shows it).
    /// `CONFLICT` when it exists, `VALIDATION` for an invalid e-mail.
    pub async fn create_service_account(
        &self,
        email: &str,
        opts: CreateServiceAccountOptions,
    ) -> Result<ServiceAccountToken> {
        let op = "admin.createServiceAccount";
        let email = check_email(email, op)?;
        let body = send_json(
            self.client
                .request(Method::POST, &self.client.org_path(&["service_accounts"]))
                .json(&json!({
                    "email": email,
                    "first_name": opts.first_name.unwrap_or_default(),
                    "last_name": opts.last_name.unwrap_or_default(),
                }))
                .operation(op),
            op,
        )
        .await?;
        token_of(&body, &email)
    }

    /// Lists the service accounts of the organization.
    pub async fn list_service_accounts(&self) -> Result<Vec<ServiceAccount>> {
        let op = "admin.listServiceAccounts";
        let body = send_json(
            self.client
                .request(Method::GET, &self.client.org_path(&["service_accounts"]))
                .operation(op),
            op,
        )
        .await?;
        Ok(items(field(&body, "data"))
            .iter()
            .map(|a| {
                let created = int(field(a, "created_at"));
                ServiceAccount {
                    email: text(field(a, "email")).unwrap_or_default(),
                    first_name: text(field(a, "first_name")).unwrap_or_default(),
                    last_name: text(field(a, "last_name")).unwrap_or_default(),
                    role: text(field(a, "role")).unwrap_or_default(),
                    created_at: (created > 0).then(|| micros_to_system_time(created)),
                }
            })
            .collect())
    }

    /// Issues a new token for a service account; the old one stops working at once.
    pub async fn rotate_service_account_token(&self, email: &str) -> Result<ServiceAccountToken> {
        let op = "admin.rotateServiceAccountToken";
        let email = check_email(email, op)?;
        let body = send_json(
            self.client
                .request(
                    Method::PUT,
                    &self.client.org_path(&["service_accounts", &email]),
                )
                .query("rotateToken", "true")
                .json(&json!({}))
                .operation(op),
            op,
        )
        .await?;
        token_of(&body, &email)
    }

    /// Removes a service account; `NOT_FOUND` when missing.
    pub async fn delete_service_account(&self, email: &str) -> Result<()> {
        let op = "admin.deleteServiceAccount";
        let email = check_email(email, op)?;
        send_json(
            self.client
                .request(
                    Method::DELETE,
                    &self.client.org_path(&["service_accounts", &email]),
                )
                .operation(op),
            op,
        )
        .await?;
        Ok(())
    }

    /// The ingest-only passcode of the current user.
    pub async fn get_passcode(&self) -> Result<Passcode> {
        let op = "admin.getPasscode";
        let body = send_json(
            self.client
                .request(Method::GET, &self.client.org_path(&["passcode"]))
                .operation(op),
            op,
        )
        .await?;
        let data = field(&body, "data");
        let passcode = text(field(data, "passcode"))
            .ok_or_else(|| Error::validation("admin.getPasscode: the response has no passcode"))?;
        self.client
            .transport()
            .add_known_secret(&Secret::new(passcode.clone()));
        Ok(Passcode {
            user: text(field(data, "user")).unwrap_or_default(),
            passcode: Secret::new(passcode),
        })
    }
}

fn check_email(email: &str, operation: &str) -> Result<String> {
    let trimmed = email.trim();
    if trimmed.is_empty() {
        return Err(Error::validation(format!(
            "{operation}: email must not be empty"
        )));
    }
    Ok(trimmed.to_string())
}

fn token_of(body: &Value, email: &str) -> Result<ServiceAccountToken> {
    let token = text(field(body, "token"))
        .ok_or_else(|| Error::validation("admin.serviceAccount: the response has no token"))?;
    Ok(ServiceAccountToken {
        user: text(field(body, "user")).unwrap_or_else(|| email.to_string()),
        token: Secret::new(token),
    })
}
