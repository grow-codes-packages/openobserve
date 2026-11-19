# Admin

Helpers for organizations, service accounts and the ingestion passcode. The integration suite
uses them to create a dedicated service account; they need a user with the right permissions
(the root user in a default install).

| Function | Request | Result |
| - | - | - |
| `listOrganizations()` | `GET /api/organizations` | `{identifier, name, type}` per organization |
| `createServiceAccount(email, {firstName, lastName})` | `POST /api/{org}/service_accounts` | `{email, token}`; the **token is returned only here** and is a redacted secret |
| `listServiceAccounts()` | `GET /api/{org}/service_accounts` | e-mails and names; the server **masks** the token, which does not authenticate |
| `rotateServiceAccountToken(email)` | `PUT /api/{org}/service_accounts/{email}?rotateToken=true` | a new `{email, token}` |
| `deleteServiceAccount(email)` | `DELETE /api/{org}/service_accounts/{email}` | — |
| `getPasscode()` | `GET /api/{org}/passcode` | `{user, passcode}` (secret) |

Returned tokens and passcodes are added to the client's redaction list, so they never appear in
errors, hook payloads or logs. Use a token together with its service-account e-mail:
`user = email`, `token = token`.

=== "TypeScript"

    ```ts
    const orgs = await client.admin.listOrganizations();
    const sa = await client.admin.createServiceAccount("ci@example.com", { firstName: "CI" });
    const ci = new OpenObserveClient({ url: client.config.url, user: sa.email, token: sa.token });
    await client.admin.rotateServiceAccountToken("ci@example.com");
    await client.admin.deleteServiceAccount("ci@example.com");
    ```

=== "Rust"

    ```rust
    use grow_openobserve::admin::CreateServiceAccountOptions;

    let admin = client.admin();
    let orgs = admin.list_organizations().await?;
    let sa = admin.create_service_account("ci@example.com", CreateServiceAccountOptions::default()).await?;
    let ci = OpenObserveClient::new(
        OpenObserveConfig::builder().url("https://openobserve.example.com").user(&sa.user).token(sa.token.clone()).build()?,
    )?;
    admin.rotate_service_account_token("ci@example.com").await?;
    admin.delete_service_account("ci@example.com").await?;
    ```

=== "Go"

    ```go
    admin := client.Admin()
    orgs, err := admin.ListOrganizations(ctx)
    sa, err := admin.CreateServiceAccount(ctx, "ci@example.com", openobserve.ServiceAccountOptions{FirstName: "CI"})
    ci, err := openobserve.NewClient(openobserve.Config{
        CommonConfig: openobserve.CommonConfig{URL: client.Config().URL}, User: sa.Email, Token: sa.Token,
    })
    _, err = admin.RotateServiceAccountToken(ctx, "ci@example.com")
    err = admin.DeleteServiceAccount(ctx, "ci@example.com")
    ```

=== "Python"

    ```python
    orgs = client.admin.list_organizations()
    sa = client.admin.create_service_account("ci@example.com", first_name="CI")
    ci = OpenObserveClient(OpenObserveConfig(url=client.config.url, user=sa.email, token=sa.token))
    client.admin.rotate_service_account_token("ci@example.com")
    client.admin.delete_service_account("ci@example.com")
    ```

=== "Java"

    ```java
    Admin admin = client.admin();
    List<Organization> orgs = admin.listOrganizations();
    ServiceAccountToken sa = admin.createServiceAccount("ci@example.com", "CI", "");
    OpenObserveClient ci = OpenObserveClient.create(OpenObserveConfig.builder()
        .url(client.config().url()).user(sa.email()).token(sa.token()).build());
    admin.rotateServiceAccountToken("ci@example.com");
    admin.deleteServiceAccount("ci@example.com");
    ```
