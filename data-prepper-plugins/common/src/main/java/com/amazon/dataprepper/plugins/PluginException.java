package com.amazon.dataprepper.plugins;

public class PluginException extends RuntimeException {
    public PluginException(final Throwable cause) {
        super(cause);
    }

    public PluginException(final String message) {
        super(message);
    }

    public PluginException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
