package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Named
public class ExtensionPluginConfigurationResolver {
    private final Map<String, Object> extensionMap;

    @Inject
    public ExtensionPluginConfigurationResolver(final DataPrepperConfiguration dataPrepperConfiguration,
                                                final PipelinesDataFlowModel pipelinesDataFlowModel) {
        extensionMap = dataPrepperConfiguration.getPipelineExtensions() == null?
                new HashMap<>() : new HashMap<>(dataPrepperConfiguration.getPipelineExtensions().getExtensionMap());
        if (pipelinesDataFlowModel.getPipelineExtensions() != null) {
            extensionMap.putAll(pipelinesDataFlowModel.getPipelineExtensions().getExtensionMap());
        }
    }

    public Map<String, Object> getExtensionMap() {
        return Collections.unmodifiableMap(extensionMap);
    }
}
