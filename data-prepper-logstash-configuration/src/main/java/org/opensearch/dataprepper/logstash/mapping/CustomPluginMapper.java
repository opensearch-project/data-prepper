/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;

import java.util.List;

/**
 * Allows for custom mapping of a Logstash plugin into one or more Data Prepper plugins
 *
 * @since 1.3
 */
public interface CustomPluginMapper {
    List<PluginModel> mapPlugin(LogstashPlugin logstashPlugin);
}
