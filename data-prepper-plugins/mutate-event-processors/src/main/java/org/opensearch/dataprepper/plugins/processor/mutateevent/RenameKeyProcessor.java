/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

@DataPrepperPlugin(name = "rename_keys", pluginType = Processor.class, pluginConfigurationType = RenameKeyProcessorConfig.class)
public class RenameKeyProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(RenameKeyProcessor.class);
    private final List<RenameKeyProcessorConfig.Entry> entries;

    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public RenameKeyProcessor(final PluginMetrics pluginMetrics, final RenameKeyProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.entries = config.getEntries();
        this.expressionEvaluator = expressionEvaluator;

        config.getEntries().forEach(entry -> {
            if (entry.getRenameWhen() != null
                    && !expressionEvaluator.isValidExpressionStatement(entry.getRenameWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("rename_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax",
                                entry.getRenameWhen()));
            }
            if (entry.getFromKey() == null && entry.getFromKeyPattern() == null) {
                throw new InvalidPluginConfigurationException("Either from_key or from_key_pattern must be specified. Both cannot be set together.");
            }
            if (entry.getFromKey() != null && entry.getFromKeyPattern()  != null) {
                throw new InvalidPluginConfigurationException("Only one of from_key or from_key_pattern should be specified.");
            }
        });
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {

                for (RenameKeyProcessorConfig.Entry entry : entries) {
                    if (Objects.nonNull(entry.getRenameWhen()) && !expressionEvaluator.evaluateConditional(entry.getRenameWhen(), recordEvent)) {
                        continue;
                    }

                    if (Objects.nonNull(entry.getFromKey()) && (entry.getFromKey().equals(entry.getToKey()) || !recordEvent.containsKey(entry.getFromKey()))) {
                        continue;
                    }
                    if (!recordEvent.containsKey(entry.getToKey()) || entry.getOverwriteIfToKeyExists()) {
                        if(Objects.nonNull(entry.getFromKey())) {
                            final Object source = recordEvent.get(entry.getFromKey(), Object.class);
                            recordEvent.put(entry.getToKey(), source);
                            recordEvent.delete(entry.getFromKey());
                        }
                        if(Objects.nonNull(entry.getFromKeyCompiledPattern())) {
                            Map<String,Object> eventMap = recordEvent.toMap();
                            Pattern fromKeyCompiledPattern = entry.getFromKeyCompiledPattern();
                            for (Map.Entry<String, Object> eventEntry : eventMap.entrySet()) {
                                final String key = eventEntry.getKey();
                                final Object value = eventEntry.getValue();
                                if (fromKeyCompiledPattern.matcher(key).matches()) {
                                    recordEvent.put(entry.getToKey(), value);
                                    recordEvent.delete(key);
                                    if(!entry.getOverwriteIfToKeyExists()) break;

                                }
                            }
                        }
                    }
                }
            } catch (final Exception e) {
                LOG.atError()
                        .addMarker(EVENT)
                        .addMarker(NOISY)
                        .setMessage("There was an exception while processing Event [{}]")
                        .addArgument(recordEvent)
                        .setCause(e)
                        .log();
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
