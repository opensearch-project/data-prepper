package com.amazon.dataprepper.model.plugin;

/**
 * This exception indicates that a plugin has an invalid definition. This
 * may mean that it does not have a required constructor, is not a concrete
 * class, or not available for construction due to class access.
 *
 * @since 1.2
 */
public class InvalidPluginDefinitionException extends RuntimeException {
    public InvalidPluginDefinitionException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InvalidPluginDefinitionException(final String message) {
        super(message);
    }
}
