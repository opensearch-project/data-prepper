/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.typeconversion;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@DataPrepperPlugin(name = "type_conversion", pluginType = Processor.class, pluginConfigurationType = TypeConversionProcessorConfig.class)
public class TypeConversionProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(TypeConversionProcessor.class);
    private String key;
    final private String type;

    @DataPrepperPluginConstructor
    public TypeConversionProcessor(final PluginMetrics pluginMetrics, final TypeConversionProcessorConfig typeConversionProcessorConfig) {
        super(pluginMetrics);
        this.key = typeConversionProcessorConfig.getKey();
        this.type = typeConversionProcessorConfig.getType();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            Object keyVal = recordEvent.get(key, Object.class);
            if (keyVal != null) {
                recordEvent.delete(key);
                switch (type) {
                    case "integer" :
                        if (keyVal instanceof String) {
                            recordEvent.put(key, Long.parseLong((String)keyVal));
                        } else if (keyVal instanceof Double) {
                            recordEvent.put(key, (long)(double)keyVal);
                        } else if (keyVal instanceof Boolean) {
                            recordEvent.put(key, ((boolean)keyVal) ? 1 : 0);
                        } else {
                            throw new IllegalArgumentException("Unsupported type conversion to " + type);
                        }
                        break;
                    case "double" :
                        if (keyVal instanceof String) {
                            recordEvent.put(key, Double.parseDouble((String)keyVal));
                        } else if (keyVal instanceof Long) {
                            recordEvent.put(key, (double)(long)keyVal);
                        } else {
                            throw new IllegalArgumentException("Unsupported type conversion to " + type);
                        }
                        break;
                    case "boolean" :
                        if (keyVal instanceof String) {
                            recordEvent.put(key, Boolean.parseBoolean((String)keyVal));
                        } else if (keyVal instanceof Long) {
                            recordEvent.put(key, (((long)keyVal) == 0) ? false : true);
                        } else {
                            throw new IllegalArgumentException("Unsupported type conversion to " + type);
                        }
                        break;
                    case "string" :
                        recordEvent.put(key, keyVal.toString());
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type conversion to " + type);
                }
            }
        }
        return records;
    }

    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }
}


