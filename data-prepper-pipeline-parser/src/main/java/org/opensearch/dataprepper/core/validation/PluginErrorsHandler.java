package org.opensearch.dataprepper.core.validation;

import java.util.Collection;

public interface PluginErrorsHandler {

    public void handleErrors(final Collection<PluginError> pluginErrors);
}
