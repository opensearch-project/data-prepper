/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.s3_enricher.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.opensearch.dataprepper.plugins.s3_enricher.processor.cache.CacheFactory;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.cache.S3EnricherCacheService;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.client.S3ClientBuilderFactory;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.configuration.S3EnricherBucketOption;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.configuration.S3EnricherKeyPathOption;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.s3source.S3EnricherObjectPluginMetrics;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.s3source.S3ObjectWorker;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.s3source.S3ObjectReference;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.s3source.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.s3source.ownership.ConfigBucketOwnerProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.s3_enricher.processor.client.S3ClientFactory.createS3Client;

@DataPrepperPlugin(name = "s3_enricher", pluginType = Processor.class, pluginConfigurationType = S3EnricherProcessorConfig.class)
public class S3EnricherProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(S3EnricherProcessor.class);
    private static final long BYTES_IN_MB = 1024 * 1024;

    public static final String NUMBER_OF_RECORDS_ENRICHED_SUCCESS = "numberOfRecordsEnrichedSuccessFromS3";
    public static final String NUMBER_OF_RECORDS_ENRICHED_FAILED = "numberOfRecordsEnrichedFailerFromS3";

    private final Map<String, String> outputFileToInputFile;
    private final S3EnricherProcessorConfig s3EnricherProcessorConfig;
    private final ObjectMapper objectMapper;
    private final ExpressionEvaluator expressionEvaluator;
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final PluginSetting codecPluginSettings;
    private final PluginFactory pluginFactory;
    private final Pattern baseNamePattern;
    protected final List<String> tagsOnFailure;

    // AWS & S3 components (thread-safe, reusable)
    private final S3Client s3Client;
    private final BucketOwnerProvider bucketOwnerProvider;
    private S3ClientBuilderFactory s3ClientBuilderFactory;

    // Processing components
    private final S3EnricherObjectPluginMetrics s3EnricherObjectPluginMetrics;
    private final S3EnricherCacheService cacheService;

    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;

    @DataPrepperPluginConstructor
    public S3EnricherProcessor(final S3EnricherProcessorConfig s3EnricherProcessorConfig,
                               final PluginMetrics pluginMetrics,
                               final AwsCredentialsSupplier awsCredentialsSupplier,
                               final ExpressionEvaluator expressionEvaluator,
                               final PluginFactory pluginFactory ) {
        super(pluginMetrics);
        this.s3EnricherProcessorConfig = s3EnricherProcessorConfig;
        this.s3Client = createS3Client(s3EnricherProcessorConfig, awsCredentialsSupplier);
        this.outputFileToInputFile = new HashMap<>();
        this.objectMapper = new ObjectMapper();
        this.expressionEvaluator = expressionEvaluator;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.pluginFactory = pluginFactory;
        CacheFactory factory = new CacheFactory(s3EnricherProcessorConfig);
        this.cacheService = new S3EnricherCacheService(factory);
        this.baseNamePattern = Pattern.compile(s3EnricherProcessorConfig.getEnricherNamePattern());
        this.tagsOnFailure = s3EnricherProcessorConfig.getTagsOnFailure();

        final AwsAuthenticationAdapter awsAuthenticationAdapter = new AwsAuthenticationAdapter(awsCredentialsSupplier, s3EnricherProcessorConfig);
        final AwsCredentialsProvider credentialsProvider = awsAuthenticationAdapter.getCredentialsProvider();
        final ConfigBucketOwnerProviderFactory configBucketOwnerProviderFactory = new ConfigBucketOwnerProviderFactory(credentialsProvider);
        bucketOwnerProvider = configBucketOwnerProviderFactory.createBucketOwnerProvider(s3EnricherProcessorConfig);
        final PluginModel codecConfiguration = s3EnricherProcessorConfig.getCodec();
        codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());

        s3ClientBuilderFactory = new S3ClientBuilderFactory(s3EnricherProcessorConfig, credentialsProvider);
        s3EnricherObjectPluginMetrics = new S3EnricherObjectPluginMetrics(pluginMetrics);

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
        String whenCondition = s3EnricherProcessorConfig.getWhenCondition();
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
            final InputCodec codec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSettings);
            S3ObjectWorker s3ObjectWorker = new S3ObjectWorker(s3EnricherProcessorConfig, codec, bucketOwnerProvider, s3ClientBuilderFactory.getS3Client(), s3EnricherObjectPluginMetrics, cacheService);

            for (Record<Event> record : recordsToEnrich) {
                try {
                    processRecord(record, s3ObjectWorker);
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

    private void processRecord(Record<Event> record, S3ObjectWorker s3ObjectWorker) throws IOException {
        Event event = record.getData();

        String bucket = event.get("/s3/bucket", String.class);
        String s3Key = event.get("/s3/key", String.class);

        if (bucket == null || bucket.isBlank() || s3Key == null || s3Key.isBlank()) {
            LOG.warn("Missing bucket or key in event, skipping enrichment");
            throw new IllegalArgumentException("Missing bucket or key in event");
        }

        String enrichFromObjectKeyName = getEnrichFromObjectKey(s3Key);
        String s3Uri = String.format("s3://%s/%s", bucket, enrichFromObjectKeyName);

        // Thread-safe load-if-absent pattern
        cacheService.loadIfAbsent(s3Uri, () -> {
            try {
                s3ObjectWorker.processS3Object(S3ObjectReference.bucketAndKey(bucket, enrichFromObjectKeyName).build());
            } catch (Exception e) {
                LOG.error("Failed to load S3 object: {}", s3Uri, e);
                throw new RuntimeException("Failed to load S3 object to enrich: " + s3Uri, e);
            }
        });

        // Get correlation value from the output event
        String correlationKey = s3EnricherProcessorConfig.getCorrelationKey();
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
     * Constructs the enrichment source object key from the output key.
     *
     * @param s3Key the S3 output object key (e.g., "bedrockbatch/output/796paes2e4je/file.jsonl.out")
     * @return the enrichment source object key, or null if outputKey is invalid
     */
    private String getEnrichFromObjectKey(final String s3Key) {
        String enrichFromFileName = extractFileName(s3Key);
        if (enrichFromFileName == null) {
            LOG.warn("Could not extract filename from s3Key: {}", s3Key);
            throw new IllegalArgumentException("Could not extract filename from s3Key: " + s3Key);
        }

        return getS3IncludePrefix()
                .map(prefix -> prefix + enrichFromFileName)
                .orElse(enrichFromFileName);
    }

    /**
     * Safely retrieves the S3 scan include prefix from the configuration chain.
     *
     * @return Optional containing the prefix if present, empty Optional otherwise
     */
    private Optional<String> getS3IncludePrefix() {
        return Optional.ofNullable(s3EnricherProcessorConfig)
                .map(S3EnricherProcessorConfig::getS3EnricherBucketOption)
                .map(S3EnricherBucketOption::getS3SourceFilter)
                .map(S3EnricherKeyPathOption::getS3scanIncludePrefixOption)
                .filter(prefix -> !prefix.isBlank());
    }

    /**
     * Extracts and transforms the filename from an S3 object key.
     *
     * Input examples:
     *   - "bedrockbatch/output/796paes2e4je/test_batch_50k-2025-11-06T21-19-15Z-xxx.jsonl.out"
     *   - "filename.jsonl.out"
     *   - "simple.jsonl"
     *
     * @param outputKey the full S3 object key or just a filename
     * @return the transformed filename (e.g., "test_batch_50k.jsonl"), or original filename if pattern doesn't match
     */
    private String extractFileName(final String outputKey) {
        if (outputKey == null || outputKey.isBlank()) {
            return null;
        }

        // Get only the file name portion (handles both paths and plain filenames)
        String fileName = extractFileNameFromS3Key(outputKey);
        if (fileName == null || fileName.isBlank()) {
            LOG.debug("Could not extract filename from path: {}", outputKey);
            return null;
        }

        // Try to match the pattern and extract base name
        if (baseNamePattern == null) {
            LOG.warn("Base name pattern is not configured, returning original filename: {}", fileName);
            return fileName;
        }

        try {
            Matcher matcher = baseNamePattern.matcher(fileName);
            if (matcher.matches() && matcher.groupCount() >= 1) {
                String baseName = matcher.group(1);
                if (baseName != null && !baseName.isBlank()) {
                    return baseName + ".jsonl";
                }
            }
        } catch (Exception e) {
            LOG.error("Error matching pattern against filename: {}", fileName, e);
            return null;
        }

        // Pattern doesn't match - return original filename
        LOG.debug("Pattern did not match filename: {}, returning as-is", fileName);
        return fileName;
    }

    /**
     * Safely extracts the filename portion from a path string.
     *
     * @param s3Key the S3 key string
     * @return the filename, or null if extraction fails
     */
    private String extractFileNameFromS3Key(final String s3Key) {
        if (s3Key == null || s3Key.isBlank()) {
            return null;
        }

        String normalized = s3Key.endsWith("/") ? s3Key.substring(0, s3Key.length() - 1) : s3Key;
        return normalized.substring(normalized.lastIndexOf('/') + 1);
    }

    /**
     * Merges specified fields from the source event into the target event
     * based on the configured merge keys.
     *
     * @param targetEvent the event to enrich (output record)
     * @param sourceEvent the event containing source data (from cache)
     */
    private void mergeData(Event targetEvent, Event sourceEvent) {
        List<EventKey> mergeKeys = s3EnricherProcessorConfig.getMergeKeys();

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
            String failureMsg = String.format("Failed to merge %d/%d keys: %s",
                    failedKeys.size(), mergeKeys.size(), failedKeys);

            if (failedKeys.size() == mergeKeys.size()) {
                // All failed - always throw
                throw new RuntimeException("All merge keys failed: " + failedKeys);
            } else {
                // Partial failure - log warning
                LOG.warn(failureMsg);
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
        if (s3Client != null) {
            s3Client.close();
        }
    }
}