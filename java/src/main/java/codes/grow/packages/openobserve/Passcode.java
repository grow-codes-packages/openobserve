package codes.grow.packages.openobserve;

import codes.grow.packages.openobserve.core.Secret;

/**
 * The caller's ingest-only credential: Basic {@code user:passcode} may ingest but not search or
 * administer.
 *
 * @param user user e-mail
 * @param passcode the passcode ({@code toString} prints {@code [REDACTED]})
 */
public record Passcode(String user, Secret passcode) {}
