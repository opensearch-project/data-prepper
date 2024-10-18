package org.opensearch.dataprepper.core.validation;

import org.opensearch.dataprepper.validation.PluginError;

import java.util.Collection;

public interface PluginErrorsHandler {

    public void handleErrors(final Collection<PluginError> pluginErrors);
}
