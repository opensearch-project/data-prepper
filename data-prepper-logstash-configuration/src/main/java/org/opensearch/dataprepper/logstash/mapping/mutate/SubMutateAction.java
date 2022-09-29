/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping.mutate;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

public interface SubMutateAction {
    static String getLogstashName() {
        return "unimplemented";
    }

    void addToModel(final LogstashAttribute attribute);
    PluginModel generateModel();
}
