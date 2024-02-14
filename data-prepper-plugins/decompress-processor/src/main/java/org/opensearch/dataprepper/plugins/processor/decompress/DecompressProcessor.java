/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress;

import com.google.common.base.Charsets;
import io.micrometer.core.instrument.Counter;
import org.apache.commons.io.IOUtils;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.decompress.exceptions.DecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collection;

@DataPrepperPlugin(name = "decompress", pluginType = Processor.class, pluginConfigurationType = DecompressProcessorConfig.class)
public class DecompressProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(DecompressProcessor.class);
    static final String DECOMPRESSION_PROCESSING_ERRORS = "processingErrors";

    private final DecompressProcessorConfig decompressProcessorConfig;
    private final ExpressionEvaluator expressionEvaluator;

    private final Counter decompressionProcessingErrors;

    @DataPrepperPluginConstructor
    public DecompressProcessor(final PluginMetrics pluginMetrics,
                               final DecompressProcessorConfig decompressProcessorConfig,
                               final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.decompressProcessorConfig = decompressProcessorConfig;
        this.expressionEvaluator = expressionEvaluator;
        this.decompressionProcessingErrors = pluginMetrics.counter(DECOMPRESSION_PROCESSING_ERRORS);

        if (decompressProcessorConfig.getDecompressWhen() != null
                && !expressionEvaluator.isValidExpressionStatement(decompressProcessorConfig.getDecompressWhen())) {
            throw new InvalidPluginConfigurationException(
                    String.format("decompress_when value of %s is not a valid expression statement. " +
                            "See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax.", decompressProcessorConfig.getDecompressWhen()));
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {

            try {
                if (decompressProcessorConfig.getDecompressWhen() != null && !expressionEvaluator.evaluateConditional(decompressProcessorConfig.getDecompressWhen(), record.getData())) {
                    continue;
                }

                for (final String key : decompressProcessorConfig.getKeys()) {

                    final String compressedValue = record.getData().get(key, String.class);

                    if (compressedValue == null) {
                        continue;
                    }

                    final byte[] compressedValueAsBytes = decompressProcessorConfig.getEncodingType().getDecoderEngine().decode(compressedValue);

                    try (final InputStream inputStream = decompressProcessorConfig.getDecompressionType().getDecompressionEngine().createInputStream(new ByteArrayInputStream(compressedValueAsBytes));){

                        final String decompressedString = IOUtils.toString(inputStream, Charsets.UTF_8);
                        record.getData().put(key, decompressedString);
                    } catch (final Exception e) {
                        LOG.error("Unable to decompress key {} using decompression type {}:",
                                key, decompressProcessorConfig.getDecompressionType(), e);
                        record.getData().getMetadata().addTags(decompressProcessorConfig.getTagsOnFailure());
                        decompressionProcessingErrors.increment();
                    }
                }
            } catch (final DecodingException e) {
                LOG.error("Unable to decode key with base64: {}", e.getMessage());
                record.getData().getMetadata().addTags(decompressProcessorConfig.getTagsOnFailure());
                decompressionProcessingErrors.increment();
            } catch (final Exception e) {
                LOG.error("An uncaught exception occurred while decompressing Events", e);
                record.getData().getMetadata().addTags(decompressProcessorConfig.getTagsOnFailure());
                decompressionProcessingErrors.increment();
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
