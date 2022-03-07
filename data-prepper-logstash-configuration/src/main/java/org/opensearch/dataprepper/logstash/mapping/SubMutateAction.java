/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

public interface SubMutateAction {
    static String getLogstashName() {
        return "unimplemented";
    }

    PluginModel generateModel(final LogstashAttribute attribute);
}
