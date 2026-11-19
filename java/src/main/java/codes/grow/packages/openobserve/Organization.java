package codes.grow.packages.openobserve;

/**
 * An OpenObserve organization.
 *
 * @param id numeric id
 * @param identifier identifier used in API paths ({@code default})
 * @param name display name
 * @param type {@code default} or {@code custom}
 * @param userEmail e-mail of the owner
 */
public record Organization(
    long id, String identifier, String name, String type, String userEmail) {}
