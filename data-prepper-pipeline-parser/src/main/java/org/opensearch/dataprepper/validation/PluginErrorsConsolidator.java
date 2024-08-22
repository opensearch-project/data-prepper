package org.opensearch.dataprepper.validation;

import javax.inject.Named;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Named
public class PluginErrorsConsolidator {
    public String consolidatedErrorMessage(final List<PluginError> pluginErrors) {
        final List<String> allErrorMessages = pluginErrors.stream()
                .map(PluginError::getErrorMessage)
                .collect(Collectors.toList());
        return IntStream.range(0, allErrorMessages.size())
                .mapToObj(i -> (i + 1) + ". " + allErrorMessages.get(i))
                .collect(Collectors.joining("\n"));
    }
}
