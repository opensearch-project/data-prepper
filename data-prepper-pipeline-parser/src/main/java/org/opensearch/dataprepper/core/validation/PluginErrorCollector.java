package org.opensearch.dataprepper.core.validation;

import lombok.Getter;
import org.opensearch.dataprepper.validation.PluginError;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;

@Named
@Getter
public class PluginErrorCollector {
    private final List<PluginError> pluginErrors = new ArrayList<>();

    public void collectPluginError(final PluginError pluginError) {
        pluginErrors.add(pluginError);
    }
}
