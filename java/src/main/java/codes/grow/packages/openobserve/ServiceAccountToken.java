package codes.grow.packages.openobserve;

import codes.grow.packages.openobserve.core.Secret;

/**
 * A service account with its usable token. The token is returned only by {@link
 * Admin#createServiceAccount} and {@link Admin#rotateServiceAccountToken}; use it as {@code
 * OpenObserveConfig.builder().user(email).token(token)}.
 *
 * @param email account e-mail
 * @param token the token ({@code toString} prints {@code [REDACTED]})
 */
public record ServiceAccountToken(String email, Secret token) {}
