/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.exception.LogstashConfigurationException;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.ArrayList;
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
    protected static final String LOGSTASH_AUTODETECT_COLUMN_NAMES_ATTRIBUTE_NAME = "autodetect_column_names";
    protected static final String DATA_PREPPER_COLUMN_NAMES_SOURCE_KEY = "column_names_source_key";
    protected static final String DATA_PREPPER_COLUMN_NAMES = "column_names";
    protected static final String LOGSTASH_AUTOGENERATE_COLUMN_NAMES_ATTRIBUTE_NAME = "autogenerate_column_names";

    protected static final String LOGSTASH_SKIP_EMPTY_ROWS_ATTRIBUTE_NAME = "skip_empty_rows";
    protected static final String LOGSTASH_SKIP_EMPTY_COLUMNS_ATTRIBUTE_NAME = "skip_empty_columns";
    protected static final String LOGSTASH_SKIP_HEADER_ATTRIBUTE_NAME = "skip_header";
    protected static final String LOGSTASH_CONVERT_ATTRIBUTE_NAME = "convert";
    protected static final String LOGSTASH_AUTODETECT_COLUMN_NAMES_EXCEPTION_MESSAGE = "Data Prepper logstash conversion does not " +
            "support the autodetect_column_names setting in Logstash configuration. Consider manually specifying a Data Prepper config " +
            "file with a CSV Processor for header autodetection functionality.";
    protected static final String LOGSTASH_SKIP_EMPTY_ROWS_EXCEPTION_MESSAGE = "Data Prepper logstash conversion does not support the " +
            "skip_empty_columns setting in Logstash configuration. Consider using a delete_entries processor with the empty columns you " +
            "wish to drop.";
    protected static final String LOGSTASH_SKIP_EMPTY_COLUMNS_EXCEPTION_MESSAGE = "Data Prepper logstash conversion does not support the " +
            "skip_empty_rows setting in Logstash configuration. Consider using a drop_events processor to drop empty rows.";
    protected static final String LOGSTASH_SKIP_HEADER_EXCEPTION_MESSAGE = "Data Prepper logstash conversion does not support the " +
            "skip_header setting in Logstash configuration. Consider using a drop_events processor to drop tbe first row.";
    protected static final String LOGSTASH_CONVERT_EXCEPTION_MESSAGE = "Data Prepper logstash conversion does not support the convert " +
            "setting in Logstash configuration. This feature may be supported in the future; a separate processor to typecast Event " +
            "fields is in the Data Prepper roadmap.";
    @Override
    protected void mapCustomAttributes(final List<LogstashAttribute> logstashAttributes,
                                       final LogstashAttributesMappings logstashAttributesMappings,
                                       final Map<String, Object> pluginSettings) {
        final Optional<LogstashAttribute> autogenerateColumnNamesAttribute = findLogstashAttribute(logstashAttributes,
                LOGSTASH_AUTOGENERATE_COLUMN_NAMES_ATTRIBUTE_NAME);

        if (logstashAttributeExistsAndIsTrue(autogenerateColumnNamesAttribute)) {
            pluginSettings.put(
                    DATA_PREPPER_COLUMN_NAMES,
                    new ArrayList<String>()
            );
        }

        final Optional<LogstashAttribute> autodetectColumnNamesAttribute = findLogstashAttribute(logstashAttributes,
                LOGSTASH_AUTODETECT_COLUMN_NAMES_ATTRIBUTE_NAME);

        if (logstashAttributeExistsAndIsTrue(autodetectColumnNamesAttribute)) {
            throw new LogstashConfigurationException(LOGSTASH_AUTODETECT_COLUMN_NAMES_EXCEPTION_MESSAGE);
        }

        final Optional<LogstashAttribute> skipEmptyRowsAttribute = findLogstashAttribute(logstashAttributes,
                LOGSTASH_SKIP_EMPTY_ROWS_ATTRIBUTE_NAME);

        if (skipEmptyRowsAttribute.isPresent()) {
            throw new LogstashConfigurationException(LOGSTASH_SKIP_EMPTY_ROWS_EXCEPTION_MESSAGE);
        }

        final Optional<LogstashAttribute> skipEmptyColumnsAttribute = findLogstashAttribute(logstashAttributes,
                LOGSTASH_SKIP_EMPTY_COLUMNS_ATTRIBUTE_NAME);
        if (skipEmptyColumnsAttribute.isPresent()) {
            throw new LogstashConfigurationException(LOGSTASH_SKIP_EMPTY_COLUMNS_EXCEPTION_MESSAGE);
        }
        final Optional<LogstashAttribute> skipHeaderAttribute = findLogstashAttribute(logstashAttributes,
                LOGSTASH_SKIP_HEADER_ATTRIBUTE_NAME);
        if (skipHeaderAttribute.isPresent()) {
            throw new LogstashConfigurationException(LOGSTASH_SKIP_HEADER_EXCEPTION_MESSAGE);
        }
        final Optional<LogstashAttribute> convertAttribute = findLogstashAttribute(logstashAttributes,
                LOGSTASH_CONVERT_ATTRIBUTE_NAME);
        if (convertAttribute.isPresent()) {
            throw new LogstashConfigurationException(LOGSTASH_CONVERT_EXCEPTION_MESSAGE);
        }
    }

    private Optional<LogstashAttribute> findLogstashAttribute(final List<LogstashAttribute> logstashAttributes,
                                                              final String logstashAttributeName) {
        return logstashAttributes.stream()
                .filter(logstashAttribute -> logstashAttribute.getAttributeName().equals(logstashAttributeName))
                .findFirst();
    }

    private boolean logstashAttributeExistsAndIsTrue(final Optional<LogstashAttribute> optionalLogstashAttribute) {
        return optionalLogstashAttribute.isPresent() && optionalLogstashAttribute.get().getAttributeValue().getValue().equals(true);
    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        final int numberOfCustomAttributeNames = 6;
        final HashSet<String> names = new HashSet<>(numberOfCustomAttributeNames);
        names.add(DATA_PREPPER_COLUMN_NAMES_SOURCE_KEY);
        names.add(DATA_PREPPER_COLUMN_NAMES);
        names.add(LOGSTASH_SKIP_EMPTY_ROWS_ATTRIBUTE_NAME);
        names.add(LOGSTASH_SKIP_EMPTY_COLUMNS_ATTRIBUTE_NAME);
        names.add(LOGSTASH_SKIP_HEADER_ATTRIBUTE_NAME);
        names.add(LOGSTASH_CONVERT_ATTRIBUTE_NAME);
        return names;
    }
}
