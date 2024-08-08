package org.opensearch.dataprepper.validation;

import lombok.Getter;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Named
@Getter
public class PluginErrorCollector {
    private final List<PluginError> pluginErrors = new ArrayList<>();

    public void collectPluginError(final PluginError pluginError) {
        pluginErrors.add(pluginError);
    }

    public List<String> getAllErrorMessages() {
        return pluginErrors.stream().map(PluginError::getErrorMessage).collect(Collectors.toList());
    }

    public String getConsolidatedErrorMessage() {
        final List<String> allErrorMessages = getAllErrorMessages();

        return IntStream.range(0, allErrorMessages.size())
                .mapToObj(i -> (i + 1) + ". " + allErrorMessages.get(i))
                .collect(Collectors.joining("\n"));
    }
}
