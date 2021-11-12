/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import java.util.Map;

/**
 * Represents attribute mappings from Logstash into Data Prepper.
 *
 * @since 1.2
 */
public interface LogstashAttributesMappings {
    Map<String, String> getMappedAttributeNames();

    Map<String, Object> getAdditionalAttributes();
}
