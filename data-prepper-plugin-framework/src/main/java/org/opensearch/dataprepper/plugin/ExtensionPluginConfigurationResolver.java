/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Named
public class ExtensionPluginConfigurationResolver {
    private final Map<String, Object> combinedExtensionMap;

    private final Map<String, Object> dataPrepperConfigExtensionMap;

    @Inject
    public ExtensionPluginConfigurationResolver(final ExtensionsConfiguration extensionsConfiguration,
                                                final PipelinesDataFlowModel pipelinesDataFlowModel) {
        this.dataPrepperConfigExtensionMap = extensionsConfiguration.getPipelineExtensions() == null?
                new HashMap<>() : new HashMap<>(extensionsConfiguration.getPipelineExtensions().getExtensionMap());
        combinedExtensionMap = new HashMap<>(dataPrepperConfigExtensionMap);
        if (pipelinesDataFlowModel.getPipelineExtensions() != null) {
            combinedExtensionMap.putAll(pipelinesDataFlowModel.getPipelineExtensions().getExtensionMap());
        }
    }

    public Map<String, Object> getDataPrepperConfigExtensionMap() {
        return Collections.unmodifiableMap(dataPrepperConfigExtensionMap);
    }

    public Map<String, Object> getCombinedExtensionMap() {
        return Collections.unmodifiableMap(combinedExtensionMap);
    }
}
