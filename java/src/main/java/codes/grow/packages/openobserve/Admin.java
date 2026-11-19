package codes.grow.packages.openobserve;

import codes.grow.packages.openobserve.core.Secret;
import codes.grow.packages.openobserve.core.TransportRequest;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.internal.Api;
import codes.grow.packages.openobserve.internal.Values;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

/**
 * Organization and service-account helpers ({@code OpenObserveClient.admin()}). The open-source
 * OpenObserve build has no RBAC: every service account can ingest, query and administer; the
 * passcode is the only ingest-only credential. Every method has a blocking form and an {@code
 * …Async} form.
 */
public final class Admin {
  private static final Pattern EMAIL = Pattern.compile("[^@\\s]+@[^@\\s]+\\.[^@\\s]+");

  private final Api api;

  /**
   * Creates the module (use {@code OpenObserveClient.admin()}).
   *
   * @param api request helper
   */
  public Admin(Api api) {
    this.api = api;
  }

  /**
   * Lists the organizations visible to the caller ({@code GET /api/organizations}), including the
   * internal {@code _meta} organization.
   *
   * @return organizations
   */
  public List<Organization> listOrganizations() {
    return Api.await(listOrganizationsAsync(), "admin.listOrganizations");
  }

  /**
   * Asynchronous {@link #listOrganizations()}.
   *
   * @return future organizations
   */
  public CompletableFuture<List<Organization>> listOrganizationsAsync() {
    return api.jsonAsync(
            TransportRequest.get("/api/organizations").operation("admin.listOrganizations"))
        .thenApply(
            body -> {
              List<Organization> out = new ArrayList<>();
              for (JsonNode o : body.path("data")) {
                out.add(
                    new Organization(
                        o.path("id").asLong(0),
                        Values.text(o.get("identifier")),
                        Values.text(o.get("name")),
                        Values.text(o.get("type")),
                        Values.text(o.get("user_email"))));
              }
              return out;
            });
  }

  /**
   * Creates a service account without names.
   *
   * @param email account e-mail
   * @return the account with its token
   */
  public ServiceAccountToken createServiceAccount(String email) {
    return createServiceAccount(email, "", "");
  }

  /**
   * Creates a service account and returns its token ({@code POST /api/{org}/service_accounts}).
   *
   * @param email account e-mail
   * @param firstName first name (may be empty)
   * @param lastName last name (may be empty)
   * @return the account with its token
   * @throws codes.grow.packages.openobserve.core.ConflictException when it already exists
   * @throws ValidationException for an invalid e-mail
   */
  public ServiceAccountToken createServiceAccount(String email, String firstName, String lastName) {
    return Api.await(
        createServiceAccountAsync(email, firstName, lastName), "admin.createServiceAccount");
  }

  /**
   * Asynchronous {@link #createServiceAccount(String, String, String)}.
   *
   * @param email account e-mail
   * @param firstName first name
   * @param lastName last name
   * @return future account with token
   */
  public CompletableFuture<ServiceAccountToken> createServiceAccountAsync(
      String email, String firstName, String lastName) {
    String op = "admin.createServiceAccount";
    return Api.defer(
        () -> {
          if (email == null || !EMAIL.matcher(email).matches()) {
            throw new ValidationException(op + ": email is not a valid address");
          }
          Map<String, String> body = new LinkedHashMap<>();
          body.put("email", email);
          body.put("first_name", firstName == null ? "" : firstName);
          body.put("last_name", lastName == null ? "" : lastName);
          return token(
              TransportRequest.post(api.orgPath("/service_accounts")).json(body).operation(op));
        });
  }

  private CompletableFuture<ServiceAccountToken> token(TransportRequest.Builder req) {
    return api.jsonAsync(req)
        .thenApply(
            body -> {
              String token = Values.text(body.get("token"));
              api.addSecret(token);
              return new ServiceAccountToken(Values.text(body.get("user")), Secret.of(token));
            });
  }

  /**
   * Lists the organization's service accounts ({@code GET /api/{org}/service_accounts}).
   *
   * @return accounts (tokens are masked by the server and not returned)
   */
  public List<ServiceAccount> listServiceAccounts() {
    return Api.await(listServiceAccountsAsync(), "admin.listServiceAccounts");
  }

  /**
   * Asynchronous {@link #listServiceAccounts()}.
   *
   * @return future accounts
   */
  public CompletableFuture<List<ServiceAccount>> listServiceAccountsAsync() {
    return api.jsonAsync(
            TransportRequest.get(api.orgPath("/service_accounts"))
                .operation("admin.listServiceAccounts"))
        .thenApply(
            body -> {
              List<ServiceAccount> out = new ArrayList<>();
              for (JsonNode s : body.path("data")) {
                long created = Values.asLong(s.get("created_at"), 0);
                out.add(
                    new ServiceAccount(
                        Values.text(s.get("email")),
                        Values.text(s.get("first_name")),
                        Values.text(s.get("last_name")),
                        Values.text(s.get("role")),
                        created > 0 ? Optional.of(Values.fromMicros(created)) : Optional.empty()));
              }
              return out;
            });
  }

  /**
   * Issues a new token; the old one stops working at once ({@code PUT
   * /api/{org}/service_accounts/{email}?rotateToken=true}).
   *
   * @param email account e-mail
   * @return the account with its new token
   */
  public ServiceAccountToken rotateServiceAccountToken(String email) {
    return Api.await(rotateServiceAccountTokenAsync(email), "admin.rotateServiceAccountToken");
  }

  /**
   * Asynchronous {@link #rotateServiceAccountToken}.
   *
   * @param email account e-mail
   * @return future account with token
   */
  public CompletableFuture<ServiceAccountToken> rotateServiceAccountTokenAsync(String email) {
    String op = "admin.rotateServiceAccountToken";
    return Api.defer(
        () ->
            token(
                TransportRequest.put(api.orgPath("/service_accounts/" + emailPath(op, email)))
                    .query("rotateToken", "true")
                    .json(Map.of())
                    .operation(op)));
  }

  /**
   * Removes a service account ({@code DELETE /api/{org}/service_accounts/{email}}).
   *
   * @param email account e-mail
   * @throws codes.grow.packages.openobserve.core.NotFoundException when it does not exist
   */
  public void deleteServiceAccount(String email) {
    Api.await(deleteServiceAccountAsync(email), "admin.deleteServiceAccount");
  }

  /**
   * Asynchronous {@link #deleteServiceAccount}.
   *
   * @param email account e-mail
   * @return completes when removed
   */
  public CompletableFuture<Void> deleteServiceAccountAsync(String email) {
    String op = "admin.deleteServiceAccount";
    return Api.defer(
        () ->
            api.sendAsync(
                    TransportRequest.delete(
                            api.orgPath("/service_accounts/" + emailPath(op, email)))
                        .operation(op))
                .thenApply(r -> (Void) null));
  }

  private static String emailPath(String op, String email) {
    if (email == null || email.isBlank()) {
      throw new ValidationException(op + ": email is required");
    }
    return URLEncoder.encode(email, StandardCharsets.UTF_8).replace("+", "%20");
  }

  /**
   * Returns the caller's ingestion passcode ({@code GET /api/{org}/passcode}).
   *
   * @return the passcode
   */
  public Passcode getPasscode() {
    return Api.await(getPasscodeAsync(), "admin.getPasscode");
  }

  /**
   * Asynchronous {@link #getPasscode()}.
   *
   * @return future passcode
   */
  public CompletableFuture<Passcode> getPasscodeAsync() {
    return api.jsonAsync(
            TransportRequest.get(api.orgPath("/passcode")).operation("admin.getPasscode"))
        .thenApply(
            body -> {
              JsonNode d = body.path("data");
              String passcode = Values.text(d.get("passcode"));
              api.addSecret(passcode);
              return new Passcode(Values.text(d.get("user")), Secret.of(passcode));
            });
  }
}
