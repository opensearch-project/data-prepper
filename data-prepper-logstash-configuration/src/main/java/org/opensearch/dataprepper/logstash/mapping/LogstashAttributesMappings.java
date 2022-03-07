/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import java.util.List;
import java.util.Map;

/**
 * Represents attribute mappings from Logstash into Data Prepper.
 *
 * @since 1.2
 */
public interface LogstashAttributesMappings {
    /**
     * A map of attribute names in the Logstash configuration to
     * the correct property name in Data Prepper.
     * <p>
     * This should not return null and should return empty if not defined.
     *
     * @return A Map
     * @since 1.2
     */
    Map<String, String> getMappedAttributeNames();

    /**
     * A map of Data Prepper property names to values to set on those
     * properties.
     * <p>
     * This should not return null and should return empty if not defined.
     *
     * @return A Map
     * @since 1.2
     */
    Map<String, Object> getAdditionalAttributes();

    /**
     * A list of attribute names in the Logstash configuration which can have nested syntax.
     * properties.
     * <p>
     * This should not return null and should return empty if not defined.
     *
     * @return A Map
     * @since 1.2
     */
    List<String> getNestedSyntaxAttributeNames();

    /**
     * The name of the new plugin 
     *
     * @return A String
     * @since 1.3
     */
    String getPluginName();

    /**
     * A map of default settings of Data Prepper attributes.
     * <p>
     * This should not return null and should return empty if not defined.
     *
     * @return A Map
     * @since 1.3
     */
    Map<String, Object> getDefaultSettings();
}
