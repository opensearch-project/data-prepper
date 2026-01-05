/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3_enrich.processor;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.expression.ExpressionParsingException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.s3.common.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.s3.common.source.S3ObjectPluginMetrics;
import org.opensearch.dataprepper.plugins.s3.common.source.S3ObjectReference;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.cache.CacheFactory;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.cache.S3EnricherCacheService;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.client.S3ClientBuilderFactory;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.s3source.S3ObjectReferenceResolver;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.s3source.S3ObjectWorker;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.s3source.ownership.ConfigBucketOwnerProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

@DataPrepperPlugin(name = "s3_enrich", pluginType = Processor.class, pluginConfigurationType = S3EnrichProcessorConfig.class)
public class S3EnrichProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(S3EnrichProcessor.class);
    private static final long BYTES_IN_MB = 1024 * 1024;

    public static final String NUMBER_OF_RECORDS_ENRICHED_SUCCESS = "numberOfRecordsEnrichedSuccessFromS3";
    public static final String NUMBER_OF_RECORDS_ENRICHED_FAILED = "numberOfRecordsEnrichedFailerFromS3";

    private final S3EnrichProcessorConfig s3EnrichProcessorConfig;
    private final ExpressionEvaluator expressionEvaluator;
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final PluginSetting codecPluginSettings;
    private final PluginFactory pluginFactory;
    private final Pattern baseNamePattern;
    private final InputCodec codec;
    private final S3ObjectWorker s3ObjectWorker;
    protected final List<String> tagsOnFailure;

    // AWS & S3 components (thread-safe, reusable)
    private final BucketOwnerProvider bucketOwnerProvider;
    private final S3ObjectReferenceResolver s3ObjectReferenceResolver;
    private S3ClientBuilderFactory s3ClientBuilderFactory;

    // Processing components
    private final S3ObjectPluginMetrics s3EnrichObjectPluginMetrics;
    private final S3EnricherCacheService cacheService;

    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;

    @DataPrepperPluginConstructor
    public S3EnrichProcessor(final S3EnrichProcessorConfig s3EnrichProcessorConfig,
                               final PluginMetrics pluginMetrics,
                               final AwsCredentialsSupplier awsCredentialsSupplier,
                               final ExpressionEvaluator expressionEvaluator,
                               final PluginFactory pluginFactory ) {
        super(pluginMetrics);
        this.s3EnrichProcessorConfig = s3EnrichProcessorConfig;
        this.expressionEvaluator = expressionEvaluator;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.pluginFactory = pluginFactory;
        CacheFactory factory = new CacheFactory(s3EnrichProcessorConfig);
        this.cacheService = new S3EnricherCacheService(factory);
        this.baseNamePattern = Pattern.compile(s3EnrichProcessorConfig.getEnricherNamePattern());
        this.tagsOnFailure = s3EnrichProcessorConfig.getTagsOnFailure();

        final AwsAuthenticationAdapter awsAuthenticationAdapter = new AwsAuthenticationAdapter(awsCredentialsSupplier, s3EnrichProcessorConfig);
        final AwsCredentialsProvider credentialsProvider = awsAuthenticationAdapter.getCredentialsProvider();
        final ConfigBucketOwnerProviderFactory configBucketOwnerProviderFactory = new ConfigBucketOwnerProviderFactory(credentialsProvider);
        bucketOwnerProvider = configBucketOwnerProviderFactory.createBucketOwnerProvider(s3EnrichProcessorConfig);
        final PluginModel codecConfiguration = s3EnrichProcessorConfig.getCodec();
        codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
        this.codec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSettings);
        this.s3ObjectReferenceResolver = new S3ObjectReferenceResolver(s3EnrichProcessorConfig);

        s3ClientBuilderFactory = new S3ClientBuilderFactory(s3EnrichProcessorConfig, credentialsProvider);
        s3EnrichObjectPluginMetrics = new S3ObjectPluginMetrics(pluginMetrics);
        this.s3ObjectWorker = new S3ObjectWorker(s3EnrichProcessorConfig, codec, bucketOwnerProvider, s3ClientBuilderFactory.getS3Client(), s3EnrichObjectPluginMetrics, cacheService);

        this.numberOfRecordsSuccessCounter = pluginMetrics.counter(
                NUMBER_OF_RECORDS_ENRICHED_SUCCESS);
        this.numberOfRecordsFailedCounter = pluginMetrics.counter(
                NUMBER_OF_RECORDS_ENRICHED_FAILED);

    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        LOG.info("S3EnricherProcessor processing {} records", records.size());
        List<Record<Event>> resultRecords = new ArrayList<>();

        // Process new records
        String whenCondition = s3EnrichProcessorConfig.getWhenCondition();
        List<Record<Event>> recordsToEnrich = records.stream()
                .filter(record -> {
                    try {
                        boolean meetCondition = whenCondition == null || expressionEvaluator.evaluateConditional(whenCondition, record.getData());
                        if (!meetCondition) {
                            resultRecords.add(record);
                        }
                        return meetCondition; // Include in recordsToEnrich if true
                    } catch (ExpressionParsingException e) {
                        LOG.warn("Expression parsing failed for record: {}. Error: {}", record, e.getMessage());
                        resultRecords.add(record);
                        return false; // Skip the record on parsing failure
                    } catch (ClassCastException e) {
                        LOG.warn("Unexpected return type when evaluating condition for record: {}. Error: {}", record, e.getMessage());
                        resultRecords.add(record);
                        return false; // Skip the record on type mismatch
                    } catch (Exception e) {
                        LOG.error("Failed to evaluate conditional expression for record: {}", record, e);
                        resultRecords.add(record);
                        return false; // Skip the record if evaluation fails
                    }
                })
                .collect(Collectors.toList());

        if (recordsToEnrich.isEmpty()) {
            return records;
        }

        try {
            for (Record<Event> record : recordsToEnrich) {
                try {
                    processRecord(record);
                    numberOfRecordsSuccessCounter.increment();
                } catch (Exception e) {
                    LOG.error(NOISY, "Error processing record", e);
                    addFailureTags(record);
                    numberOfRecordsFailedCounter.increment();
                }
                resultRecords.add(record);
            }
        } catch (Exception e) {
            LOG.error(NOISY, "Error while initializing the S3 Object Worker", e);
            numberOfRecordsFailedCounter.increment(recordsToEnrich.size());
            addFailureTags(recordsToEnrich);
            resultRecords.addAll(recordsToEnrich);
        }

        return resultRecords;
    }

    private void processRecord(Record<Event> record) {
        Event event = record.getData();

        // Resolve S3ObjectReference from the event
        final S3ObjectReference s3ObjectReference = s3ObjectReferenceResolver.resolve(event);
        final String s3Uri = s3ObjectReference.uri();

        // Thread-safe load-if-absent pattern
        cacheService.loadIfAbsent(s3Uri, () -> {
            try {
                s3ObjectWorker.processS3Object(s3ObjectReference);
            } catch (Exception e) {
                LOG.error("Failed to load S3 object: {}", s3Uri, e);
                throw new RuntimeException("Failed to load S3 object to enrich: " + s3Uri, e);
            }
        });

        // Get correlation value from the output event
        String correlationKey = s3EnrichProcessorConfig.getCorrelationKeys().get(0);   // only consider one key for now
        String correlationValue = event.get(correlationKey, String.class);

        if (correlationValue == null || correlationValue.isBlank()) {
            LOG.warn("No correlation value found for key '{}' in event", correlationKey);
            throw new IllegalArgumentException("No correlation value found for key '" + correlationKey + "'");
        }

        // Lookup source event from cache
        Event enrichFromEvent = cacheService.get(s3Uri, correlationValue);
        if (enrichFromEvent != null) {
            mergeData(event, enrichFromEvent);
            LOG.debug("Successfully merged data for correlationValue: {}", correlationValue);
        } else {
            LOG.warn("No matching source record found for correlation value: {} in {}", correlationValue, s3Uri);
            throw new RuntimeException("No matching source record found for correlation value: " + correlationValue);
        }
    }

    /**
     * Merges specified fields from the source event into the target event
     * based on the configured merge keys.
     *
     * @param targetEvent the event to enrich (output record)
     * @param sourceEvent the event containing source data (from cache)
     */
    private void mergeData(Event targetEvent, Event sourceEvent) {
        List<EventKey> mergeKeys = s3EnrichProcessorConfig.getMergeKeys();

        // If no merge keys specified, merge nothing from source
        if (mergeKeys == null || mergeKeys.isEmpty()) {
            LOG.debug("No merge keys configured, does not merge anything from source");
            throw new IllegalArgumentException("No merge keys configured");
        }

        List<String> failedKeys = new ArrayList<>();
        // Merge only specified keys
        for (EventKey eventKey : mergeKeys) {
            try {
                mergeKey(targetEvent, sourceEvent, eventKey);
            } catch (Exception e) {
                LOG.error("Failed to merge key '{}': {}", eventKey, e.getMessage(), e);
                failedKeys.add(eventKey.getKey());
            }
        }
        // Handle failures based on configuration or policy
        if (!failedKeys.isEmpty()) {
            if (failedKeys.size() == mergeKeys.size()) {
                // All failed - always throw
                throw new RuntimeException("All merge keys failed: " + failedKeys);
            } else {
                // Partial failure - log warning
                LOG.warn("Failed to merge {}/{} keys: {}",
                    failedKeys.size(), mergeKeys.size(), failedKeys);
            }
        }
    }

    /**
     * Merges a single key from source to target event.
     */
    private void mergeKey(Event targetEvent, Event sourceEvent, EventKey eventKey) {
        String keyPath = eventKey.getKey();

        // Check if source has this key
        if (!sourceEvent.containsKey(keyPath)) {
            LOG.debug("Source event does not contain key: {}", keyPath);
            throw new IllegalArgumentException("Source event does not contain key: " + keyPath);
        }

        // Get value from source
        Object sourceValue = sourceEvent.get(keyPath, Object.class);

        if (sourceValue == null) {
            LOG.debug("Source value is null for key: {}", keyPath);
            throw new IllegalArgumentException("Source value is null for key: " + keyPath);
        }

        // Put into target (will overwrite if exists)
        targetEvent.put(eventKey, sourceValue);
        LOG.trace("Merged key '{}' with value: {}", keyPath, sourceValue);
    }

    /**
     * Add the failure tags to multiple records that aren't processed
     */
    protected void addFailureTags(List<Record<Event>> records) {
        if (tagsOnFailure == null || tagsOnFailure.isEmpty()) {
            return;
        }

        if (records == null || records.isEmpty()) {
            return;
        }

        for (Record<Event> record : records) {
            addFailureTags(record);
        }
    }

    /*
     * Add the failure tags to the records that aren't processed
     */
    protected void addFailureTags(Record<Event> record) {
        if (tagsOnFailure == null || tagsOnFailure.isEmpty()) {
            return;
        }
        // Add failure tags to each event
        Event event = record.getData();
        EventMetadata metadata = event.getMetadata();
        if (metadata != null) {
            metadata.addTags(tagsOnFailure);
        } else {
            LOG.warn("Event metadata is null, cannot add failure tags.");
        }
    }

    @Override
    public void prepareForShutdown() {}

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }
}