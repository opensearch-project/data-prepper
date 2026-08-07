/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.detect_format;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;

import java.util.Collection;
import java.util.List;

@DataPrepperPlugin(name = "detect_format", pluginType = Processor.class, pluginConfigurationType = DetectFormatProcessorConfig.class)
public class DetectFormatProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private String source;
    private String targetKey;
    private String targetMetadataKey;
    private String detectWhen;
    private List<String> kvSeparatorList;
    private String kvDelimiter;
    private ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public DetectFormatProcessor(final DetectFormatProcessorConfig detectFormatProcessorConfig, final PluginMetrics pluginMetrics, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        source = detectFormatProcessorConfig.getSource();
        targetKey = detectFormatProcessorConfig.getTargetKey();
        targetMetadataKey = detectFormatProcessorConfig.getTargetMetadataKey();
        detectWhen = detectFormatProcessorConfig.getDetectWhen();
        kvSeparatorList = detectFormatProcessorConfig.getKVSeparatorList();
        kvDelimiter = detectFormatProcessorConfig.getKVDelimiter();
        this.expressionEvaluator = expressionEvaluator;
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        String format = null;
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            if (detectWhen != null && !expressionEvaluator.evaluateConditional(detectWhen, recordEvent)) {
                continue;
            }
            // check for exception here?
            String sourceData = recordEvent.get(source, String.class);
            if (sourceData == null) {
                continue;
            }
            sourceData = sourceData.trim();
        
            // JSON: Starts with {  and ends with }
            if (sourceData.startsWith("{") && sourceData.endsWith("}")) {
                format = "json";
            } else if (sourceData.startsWith("<?xml") ||
                ((sourceData.startsWith("<")) && sourceData.endsWith(">"))) {
                // XML: Starts with < and ends with > or has an XML declaration
                format = "xml";
            } else { 
                String[] lines = sourceData.split("\r?\n");
                if (lines.length > 1) {
                    long expectedCommas = lines[0].chars().filter(ch -> ch == ',').count();
    
                    int maxMatches = Math.min(lines.length, 10);
                    int numMatches = 0;
                    // Limit to the check to max of 10 lines
                    for (int i = 1; i < maxMatches; i++) {
                        long commas = lines[i].chars().filter(ch -> ch == ',').count();
                        if (commas == expectedCommas)
                            numMatches++;
                    }
                    if (numMatches >= maxMatches/2) {
                        format = "csv";
                    }
                } 
                if (format == null) {
                    for (final String kvSeparator : kvSeparatorList) {
                        String[] keyValuePairs = lines[0].split(kvSeparator);
                        if ((keyValuePairs.length > 1) && (keyValuePairs[0].contains(kvDelimiter))) {
                            format="keyvalue";
                            break;
                        }
                    }
                }
            }
            if (format != null) {
                if (targetKey != null) {
                    recordEvent.put(targetKey, format);
                }
                if (targetMetadataKey != null) {
                    recordEvent.getMetadata().setAttribute(targetMetadataKey, format);
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

