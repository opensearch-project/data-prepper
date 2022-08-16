/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * LogstashPluginAttributesMapper that maps Logstash's csv filter to Data Prepper's csv processor.
 * In addition to the settings in csv.mapping.yaml, this mapper supports Logstash's autogenerate_column_names setting.
 *
 */
public class CsvLogstashPluginAttributesMapper extends AbstractLogstashPluginAttributesMapper{
    protected static final String LOGSTASH_AUTOGENERATE_COLUMN_NAMES_ATTRIBUTE_NAME = "autogenerate_column_names";
    protected static final String LOGSTASH_COLUMNS_ATTRIBUTE_NAME = "columns";
    protected static final String DATA_PREPPER_COLUMN_NAMES = "column_names";

    @Override
    protected void mapCustomAttributes(final List<LogstashAttribute> logstashAttributes,
                                       final LogstashAttributesMappings logstashAttributesMappings,
                                       final Map<String, Object> pluginSettings) {


        final Optional<LogstashAttribute> autogenerateColumnNamesAttribute = findLogstashAttribute(logstashAttributes,
                LOGSTASH_AUTOGENERATE_COLUMN_NAMES_ATTRIBUTE_NAME);
        final Object autogenerateColumnNamesValue = autogenerateColumnNamesAttribute.map(attribute -> attribute.getAttributeValue().getValue())
                .orElse(false);
        final boolean isAutogenerateColumnNames = autogenerateColumnNamesValue.equals(true);

        final Optional<LogstashAttribute> columnsAttribute = findLogstashAttribute(logstashAttributes,
                LOGSTASH_COLUMNS_ATTRIBUTE_NAME);
        final List<String> columnsValue = (List<String>) columnsAttribute.map(attribute -> attribute.getAttributeValue().getValue())
                .orElse(Collections.emptyList());
        final boolean columnsValueIsEmptyOrDoesNotExist = columnsValue.isEmpty();

        if (isAutogenerateColumnNames && columnsValueIsEmptyOrDoesNotExist) {
            pluginSettings.put(
                    DATA_PREPPER_COLUMN_NAMES,
                    new ArrayList<String>()
            );
        }
    }

    private Optional<LogstashAttribute> findLogstashAttribute(final List<LogstashAttribute> logstashAttributes,
                                                              final String logstashAttributeName) {
        return logstashAttributes.stream()
                .filter(logstashAttribute -> logstashAttribute.getAttributeName().equals(logstashAttributeName))
                .findFirst();
    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        return new HashSet<>(Collections.singleton(LOGSTASH_AUTOGENERATE_COLUMN_NAMES_ATTRIBUTE_NAME));
    }
}
