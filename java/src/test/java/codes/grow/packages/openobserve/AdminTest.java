package codes.grow.packages.openobserve;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.openobserve.core.ConflictException;
import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.MockServer;
import codes.grow.packages.openobserve.core.NotFoundException;
import codes.grow.packages.openobserve.core.ValidationException;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AdminTest {
  private MockServer server;
  private OpenObserveClient client;
  private Admin admin;

  @BeforeEach
  void start() throws Exception {
    server = MockServer.start();
    client = Fixtures.client(server).build();
    admin = client.admin();
  }

  @AfterEach
  void stop() {
    client.close();
    server.close();
  }

  @Test
  void organizations() {
    Fixtures.enqueue(server, "listOrganizations.ok");
    List<Organization> orgs = admin.listOrganizations();
    assertThat(orgs).extracting(Organization::identifier).containsExactly("_meta", "default");
    assertThat(orgs.get(1).type()).isEqualTo("default");
    assertThat(orgs.get(1).id()).isEqualTo(1);
    assertThat(orgs.get(1).userEmail()).isEqualTo("admin@stack.local");
    assertThat(server.requests().get(0).uri()).isEqualTo("/api/organizations");
  }

  @Test
  void serviceAccountLifecycle() {
    server.enqueue(200, "{\"code\":200,\"token\":\"sa-token-abcdef\",\"user\":\"sa@stack.local\"}");
    Fixtures.enqueue(server, "createServiceAccount.400_duplicate");
    Fixtures.enqueue(server, "createServiceAccount.400_invalid_email");
    Fixtures.enqueue(server, "listServiceAccounts.ok");
    server.enqueue(200, "{\"token\":\"rotated-token-1234\",\"user\":\"sa@stack.local\"}");
    Fixtures.enqueue(server, "deleteServiceAccount.ok");
    Fixtures.enqueue(server, "deleteServiceAccount.404");
    ServiceAccountToken t = admin.createServiceAccount("sa@stack.local", "SDK", "Tests");
    assertThat(t.email()).isEqualTo("sa@stack.local");
    assertThat(t.token().reveal()).isEqualTo("sa-token-abcdef");
    assertThat(t.toString()).doesNotContain("sa-token-abcdef");
    JsonNode sent = Json.tree(server.requests().get(0).body().getBytes(StandardCharsets.UTF_8));
    assertThat(sent.get("first_name").asText()).isEqualTo("SDK");
    assertThat(client.transport().config().knownSecrets()).contains("sa-token-abcdef");
    assertThatThrownBy(() -> admin.createServiceAccount("sa@stack.local"))
        .isInstanceOf(ConflictException.class)
        .hasMessageContaining("User already exists");
    assertThatThrownBy(() -> admin.createServiceAccountAsync("x@y.z", null, null).join())
        .hasCauseInstanceOf(ValidationException.class);
    List<ServiceAccount> list = admin.listServiceAccounts();
    assertThat(list).hasSize(2);
    assertThat(list.get(0).role()).isEqualTo("service_account");
    assertThat(list.get(0).createdAt()).isPresent();
    assertThat(list.get(0).firstName()).isEqualTo("SDK");
    ServiceAccountToken rotated = admin.rotateServiceAccountToken("sa@stack.local");
    assertThat(rotated.token().reveal()).isEqualTo("rotated-token-1234");
    MockServer.Recorded put = server.requests().get(4);
    assertThat(put.method()).isEqualTo("PUT");
    assertThat(put.uri())
        .isEqualTo("/api/default/service_accounts/sa%40stack.local?rotateToken=true");
    admin.deleteServiceAccount("sa@stack.local");
    assertThat(server.requests().get(5).method()).isEqualTo("DELETE");
    assertThatThrownBy(() -> admin.deleteServiceAccountAsync("sa@stack.local").join())
        .hasCauseInstanceOf(NotFoundException.class);
    assertThatThrownBy(() -> admin.createServiceAccount("not-an-email"))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> admin.createServiceAccount(null))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> admin.rotateServiceAccountToken(" "))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> admin.deleteServiceAccount(null))
        .isInstanceOf(ValidationException.class);
    assertThat(new ServiceAccount("e", "f", "l", "r", null).createdAt()).isEmpty();
  }

  @Test
  void passcode() {
    server.enqueue(
        200, "{\"data\":{\"passcode\":\"pass-code-xyz\",\"user\":\"admin@stack.local\"}}");
    server.enqueue(200, "{\"data\":[{\"email\":\"e\"}]}");
    Passcode p = admin.getPasscode();
    assertThat(p.user()).isEqualTo("admin@stack.local");
    assertThat(p.passcode().reveal()).isEqualTo("pass-code-xyz");
    assertThat(client.transport().config().knownSecrets()).contains("pass-code-xyz");
    assertThat(admin.listServiceAccounts().get(0).createdAt()).isEmpty();
  }
}
