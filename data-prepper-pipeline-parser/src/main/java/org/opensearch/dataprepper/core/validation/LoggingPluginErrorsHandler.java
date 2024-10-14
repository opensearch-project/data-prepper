package org.opensearch.dataprepper.core.validation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Named
public class LoggingPluginErrorsHandler implements PluginErrorsHandler {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingPluginErrorsHandler.class);

    @Override
    public void handleErrors(final Collection<PluginError> pluginErrors) {
        final List<String> allErrorMessages = pluginErrors.stream()
                .map(PluginError::getErrorMessage)
                .collect(Collectors.toList());
        final String consolidatedErrorMessage = IntStream.range(0, allErrorMessages.size())
                .mapToObj(i -> (i + 1) + ". " + allErrorMessages.get(i))
                .collect(Collectors.joining("\n"));
        LOG.error(consolidatedErrorMessage);
    }
}
