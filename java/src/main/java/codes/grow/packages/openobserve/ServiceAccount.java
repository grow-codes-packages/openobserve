package codes.grow.packages.openobserve;

import java.time.Instant;
import java.util.Optional;

/**
 * A listed service account (the server masks its token, so it is not returned).
 *
 * @param email account e-mail
 * @param firstName first name
 * @param lastName last name
 * @param role role ({@code service_account})
 * @param createdAt creation time
 */
public record ServiceAccount(
    String email, String firstName, String lastName, String role, Optional<Instant> createdAt) {
  /**
   * Normalizes a null creation time.
   *
   * @param email e-mail
   * @param firstName first name
   * @param lastName last name
   * @param role role
   * @param createdAt creation time
   */
  public ServiceAccount {
    createdAt = createdAt == null ? Optional.empty() : createdAt;
  }
}
