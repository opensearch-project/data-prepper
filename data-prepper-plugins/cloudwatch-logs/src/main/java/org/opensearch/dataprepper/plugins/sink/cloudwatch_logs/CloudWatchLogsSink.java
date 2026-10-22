/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.Batch;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.BatchSender;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsService;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsClientFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.EntityResolver;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.OutcomeHandler;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.ParallelFlusher;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.EntityConfig;

import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.exception.InvalidBufferTypeException;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsLimits;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsSinkUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

@Experimental
@DataPrepperPlugin(name = "cloudwatch_logs", pluginType = Sink.class, pluginConfigurationType = CloudWatchLogsSinkConfig.class)
public class CloudWatchLogsSink extends AbstractSink<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsSink.class);

    static final Duration TERMINATION_WAIT = CloudWatchLogsClientFactory.API_CALL_TIMEOUT.plusSeconds(5);

    // Guards the group buffers. Never held during I/O, backoff, the DLQ write or a flusher wait.
    private final ReentrantLock processLock = new ReentrantLock();
    // Also keeps the cardinality gauge's weak reference to the service from being collected.
    private final CloudWatchLogsService cloudWatchLogsService;
    private final CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private final CloudWatchLogsLimits cloudWatchLogsLimits;
    private final ThreadPoolExecutor flusherPool;
    private final BatchSender batchSender;
    private final OutcomeHandler outcomeHandler;
    private final ParallelFlusher parallelFlusher;
    private DlqPushHandler dlqPushHandler = null;
    private volatile boolean isInitialized;

    @DataPrepperPluginConstructor
    public CloudWatchLogsSink(final PluginSetting pluginSetting,
                              final PluginMetrics pluginMetrics,
                              final PluginFactory pluginFactory,
                              final CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig,
                              final AwsCredentialsSupplier awsCredentialsSupplier,
                              final ExpressionEvaluator expressionEvaluator) {
        this(pluginSetting, pluginMetrics, pluginFactory, cloudWatchLogsSinkConfig, awsCredentialsSupplier, expressionEvaluator,
                (ThreadPoolExecutor) Executors.newFixedThreadPool(cloudWatchLogsSinkConfig.getWorkers(),
                        BackgroundThreadFactory.defaultExecutorThreadFactory("cloudwatch-logs-flusher")), new Random(),
                System::currentTimeMillis);
    }

    CloudWatchLogsSink(final PluginSetting pluginSetting,
                       final PluginMetrics pluginMetrics,
                       final PluginFactory pluginFactory,
                       final CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig,
                       final AwsCredentialsSupplier awsCredentialsSupplier,
                       final ExpressionEvaluator expressionEvaluator,
                       final ThreadPoolExecutor flusherPool,
                       final Random random,
                       final LongSupplier clock) {
        super(pluginSetting);

        AwsConfig awsConfig = cloudWatchLogsSinkConfig.getAwsConfig();
        ThresholdConfig thresholdConfig = cloudWatchLogsSinkConfig.getThresholdConfig();
        Map<String, String> headerOverrides = cloudWatchLogsSinkConfig.getHeaderOverrides();

        // Literal headers are applied once at client build; templated headers are resolved per request and
        // must be kept off the client, or they would be sent verbatim.
        final Map<String, String> staticHeaders = new HashMap<>();
        final Map<String, String> dynamicHeaderTemplates = new LinkedHashMap<>();
        for (final Map.Entry<String, String> header : headerOverrides.entrySet()) {
            if (CloudWatchLogsSinkUtils.isDynamicExpression(header.getValue(), expressionEvaluator)) {
                dynamicHeaderTemplates.put(header.getKey(), header.getValue());
            } else {
                staticHeaders.put(header.getKey(), header.getValue());
            }
        }

        // Log custom headers configuration during plugin startup
        logCustomHeadersConfiguration(headerOverrides);

        cloudWatchLogsMetrics = new CloudWatchLogsMetrics(pluginMetrics);
        cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(),
                thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSizeBytes(), thresholdConfig.getFlushInterval());

        if (awsConfig == null && awsCredentialsSupplier == null) {
            throw new RuntimeException("Missing awsConfig and awsCredentialsSupplier");
        }
        CloudWatchLogsClient cloudWatchLogsClient = CloudWatchLogsClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier, staticHeaders, cloudWatchLogsSinkConfig.getEndpoint());
        if (cloudWatchLogsClient == null) {
            throw new RuntimeException("cloudWatchLogsClient is null");
        }

        BufferFactory bufferFactory = null;
        bufferFactory = new InMemoryBufferFactory();

        if (cloudWatchLogsSinkConfig.getDlq() != null) {
            String region = null;
            if (awsConfig != null && awsConfig.getAwsRegion() != null) {
                region = awsConfig.getAwsRegion().toString();
            } else if (awsCredentialsSupplier != null) {
                region = awsCredentialsSupplier.getDefaultRegion()
                        .map(Region::toString)
                        .orElse(null);
            }
            String role = null;
            if (awsConfig != null && awsConfig.getAwsStsRoleArn() != null) {
                role = awsConfig.getAwsStsRoleArn();
            } else if (awsCredentialsSupplier != null) {
                role = awsCredentialsSupplier.getDefaultStsRoleArn()
                        .orElse(null);
            }
            dlqPushHandler = new DlqPushHandler(pluginFactory, pluginSetting, pluginMetrics, cloudWatchLogsSinkConfig.getDlq(), region, role, "cloudWatchLogs");
        }

        this.flusherPool = flusherPool;
        cloudWatchLogsMetrics.registerExecutorGauges(flusherPool);

        final EntityConfig entityConfig = cloudWatchLogsSinkConfig.getEntityConfig();
        final boolean dynamicEntity = entityConfig != null && entityConfig.isDynamic(expressionEvaluator);
        final boolean dynamicHeaders = !dynamicHeaderTemplates.isEmpty();
        // Events are partitioned into per-group requests whenever the entity or the headers vary per event.
        final boolean grouped = dynamicEntity || dynamicHeaders;

        final Entity staticEntity = (entityConfig == null || dynamicEntity) ? null : Entity.builder()
                .keyAttributes(entityConfig.getKeyAttributes())
                .attributes(entityConfig.getAttributes())
                .build();

        // The same evaluator that classified this config as dynamic has to do the interpolating, or
        // expression-valued attributes would resolve to an empty string instead of being evaluated. Key and
        // attribute templates are only supplied when the entity itself is dynamic; otherwise the resolver
        // partitions purely by the dynamic headers and uses staticEntity for every group.
        final EntityResolver entityResolver = grouped
                ? new EntityResolver(
                        dynamicEntity ? entityConfig.getKeyAttributes() : Map.of(),
                        dynamicEntity ? entityConfig.getAttributes() : Map.of(),
                        dynamicHeaderTemplates,
                        expressionEvaluator,
                        cloudWatchLogsMetrics)
                : null;
        final int maxEntityCardinality = entityConfig != null
                ? entityConfig.getMaxCardinality()
                : EntityConfig.DEFAULT_MAX_CARDINALITY;

        batchSender = new BatchSender(cloudWatchLogsClient, cloudWatchLogsMetrics, cloudWatchLogsSinkConfig.getMaxRetries(),
                cloudWatchLogsSinkConfig.getCreateLogGroup(), cloudWatchLogsSinkConfig.getCreateLogStream(), random, clock);
        outcomeHandler = new OutcomeHandler(dlqPushHandler, cloudWatchLogsMetrics);
        parallelFlusher = new ParallelFlusher(flusherPool, batchSender, outcomeHandler,
                cloudWatchLogsSinkConfig.getLogGroup(), cloudWatchLogsSinkConfig.getLogStream());

        if (grouped) {
            cloudWatchLogsService = new CloudWatchLogsService(bufferFactory, cloudWatchLogsMetrics, cloudWatchLogsLimits,
                    entityResolver, maxEntityCardinality, staticEntity);
            // Gauged on the service: the group count is the number of groups actually being buffered
            // right now, and it falls again as groups go idle rather than only ever climbing.
            cloudWatchLogsMetrics.registerEntityCardinalityGauge(cloudWatchLogsService,
                    CloudWatchLogsService::activeEntityGroupCount);
        } else {
            Buffer buffer;
            try {
                buffer = bufferFactory.getBuffer();
            } catch (NullPointerException e) {
                throw new InvalidBufferTypeException("Error loading buffer!");
            }
            cloudWatchLogsService = new CloudWatchLogsService(buffer, cloudWatchLogsMetrics, cloudWatchLogsLimits, staticEntity);
        }
    }

    @Override
    public void doInitialize() {
        isInitialized = Boolean.TRUE;
    }

    ReentrantLock getProcessLock() {
        return processLock;
    }

    /**
     * Returns only once every batch sent by this call is settled. Never propagates an {@link Exception} or
     * {@link StackOverflowError}.
     */
    @Override
    public void doOutput(final Collection<Record<Event>> records) {
        final Set<EventHandle> released = Collections.newSetFromMap(new IdentityHashMap<>());
        final Set<EventHandle> ownedByBuffer = Collections.newSetFromMap(new IdentityHashMap<>());
        final List<Batch> batches = new ArrayList<>();
        final List<SerializedEvent> deliverable = new ArrayList<>(records.size());
        final List<OutcomeHandler.FailedEvent> undeliverable = new ArrayList<>();
        final List<OutcomeHandler.FailedEvent> unexpected = new ArrayList<>();
        final List<OutcomeHandler.FailedEvent> refused = new ArrayList<>();
        EventHandle addInProgress = null;
        try {
            for (final Record<Event> record : records) {
                final Event event = eventOf(record);
                if (event == null) {
                    LOG.warn("Skipping a record with no event");
                    continue;
                }
                try {
                    final String logString = event.toJsonString();
                    final byte[] logBytes = logString.getBytes(StandardCharsets.UTF_8);
                    cloudWatchLogsMetrics.recordLogSize(logBytes.length);
                    if (cloudWatchLogsLimits.isGreaterThanMaxEventSize(logBytes.length)) {
                        undeliverable.add(new OutcomeHandler.FailedEvent(event.getEventHandle(), logString,
                                String.format("Event blocked due to Max Size restriction! Event Size : %s",
                                        logBytes.length + CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE)));
                    } else {
                        deliverable.add(new SerializedEvent(event, logString, logBytes));
                    }
                } catch (final Exception | StackOverflowError e) {
                    LOG.error("Failed to serialize an event", e);
                    unexpected.add(new OutcomeHandler.FailedEvent(event.getEventHandle(), null, e.getMessage()));
                }
            }

            processLock.lock();
            try {
                if (batchSender.isClosed()) {
                    for (final SerializedEvent serialized : deliverable) {
                        refused.add(new OutcomeHandler.FailedEvent(serialized.event.getEventHandle(), serialized.message, null));
                    }
                } else {
                    for (final SerializedEvent serialized : deliverable) {
                        final EventHandle eventHandle = serialized.event.getEventHandle();
                        addInProgress = eventHandle;
                        try {
                            cloudWatchLogsService.add(serialized.event, serialized.bytes, batches);
                            ownedByBuffer.add(eventHandle);
                        } catch (final Exception | StackOverflowError e) {
                            LOG.error("Failed to buffer an event", e);
                            unexpected.add(new OutcomeHandler.FailedEvent(eventHandle, serialized.message, e.getMessage()));
                        }
                        addInProgress = null;
                    }
                    cloudWatchLogsService.takeReady(batches);
                }
            } finally {
                processLock.unlock();
            }

            outcomeHandler.terminalStep(released, OutcomeHandler.Reason.UNDELIVERABLE, undeliverable);
            outcomeHandler.terminalStep(released, OutcomeHandler.Reason.UNEXPECTED_ERROR, unexpected);
            outcomeHandler.terminalStep(released, OutcomeHandler.Reason.SHUTDOWN, refused);
            parallelFlusher.flush(batches);
        } catch (final Exception | StackOverflowError e) {
            LOG.error("Unexpected error in doOutput; settling every event this call still owns", e);
            outcomeHandler.settleUnsettled(batches,
                    batchSender.isClosed() ? OutcomeHandler.Reason.SHUTDOWN : OutcomeHandler.Reason.UNEXPECTED_ERROR, e);
            if (outcomeHandler.releaseUnreleased(released, handlesNotOwnedByBuffer(records, ownedByBuffer, null)) > 0) {
                cloudWatchLogsMetrics.increaseUnhandledErrorCounter(1);
            }
        } catch (final Error e) {
            outcomeHandler.releaseUnsettled(batches);
            // An add that threw an Error may or may not have buffered its event, so that event is left for the source to redrive.
            outcomeHandler.releaseUnreleased(released, handlesNotOwnedByBuffer(records, ownedByBuffer, addInProgress));
            throw e;
        }
    }

    /**
     * Stops accepting events, sends everything still buffered with at most one attempt per batch, and shuts the flusher pool down.
     * Idempotent: a second or concurrent call returns immediately, without waiting for the first call to finish.
     */
    @Override
    public void shutdown() {
        super.shutdown();
        boolean interrupted = Thread.interrupted();
        try {
            final List<Batch> batches = new ArrayList<>();
            Error error = null;
            try {
                processLock.lock();
                try {
                    if (batchSender.isClosed()) {
                        return;
                    }
                    batchSender.close();
                    cloudWatchLogsService.takeAll(batches);
                } finally {
                    processLock.unlock();
                }
                parallelFlusher.flush(batches);
            } catch (final Exception | StackOverflowError e) {
                LOG.error("Unexpected error flushing on shutdown; settling the remaining batches with reason shutdown", e);
                takeRemaining(batches);
                outcomeHandler.settleUnsettled(batches, OutcomeHandler.Reason.SHUTDOWN, e);
            } catch (final Error e) {
                error = e;
            }
            interrupted |= Thread.interrupted();
            interrupted |= shutdownFlusherPool();
            if (error != null) {
                outcomeHandler.releaseUnsettled(batches);
                throw error;
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * A takeAll that threw part-way leaves the groups it had not reached in the buffer, and nothing else takes them once closed is
     * set, so one more takeAll collects them to be settled as shutdown. Groups already taken are empty and are skipped.
     */
    private void takeRemaining(final List<Batch> batches) {
        processLock.lock();
        try {
            cloudWatchLogsService.takeAll(batches);
        } catch (final Exception | StackOverflowError e) {
            LOG.error("Unable to take the remaining buffered events on shutdown; they are left for the source to redrive", e);
        } finally {
            processLock.unlock();
        }
    }

    /**
     * Never calls shutdownNow, so in-flight sends still settle their batches. An interrupted wait is retried with the remaining time.
     *
     * @return whether the wait was interrupted
     */
    private boolean shutdownFlusherPool() {
        flusherPool.shutdown();
        final long deadline = System.nanoTime() + TERMINATION_WAIT.toNanos();
        boolean interrupted = false;
        boolean terminated = false;
        long remaining = TERMINATION_WAIT.toNanos();
        while (remaining > 0) {
            try {
                terminated = flusherPool.awaitTermination(remaining, TimeUnit.NANOSECONDS);
                break;
            } catch (final InterruptedException e) {
                interrupted = true;
                remaining = deadline - System.nanoTime();
            }
        }
        if (!terminated && !flusherPool.isTerminated()) {
            LOG.error("CloudWatch Logs flusher pool did not terminate within {}; {} threads are still active and settle their batches "
                    + "when they finish", TERMINATION_WAIT, flusherPool.getActiveCount());
        }
        return interrupted;
    }

    private static List<EventHandle> handlesNotOwnedByBuffer(final Collection<Record<Event>> records,
                                                             final Set<EventHandle> ownedByBuffer, final EventHandle excluded) {
        final List<EventHandle> handles = new ArrayList<>(records.size());
        for (final Record<Event> record : records) {
            final Event event = eventOf(record);
            if (event == null) {
                continue;
            }
            final EventHandle eventHandle = event.getEventHandle();
            if (eventHandle != excluded && !ownedByBuffer.contains(eventHandle)) {
                handles.add(eventHandle);
            }
        }
        return handles;
    }

    private static Event eventOf(final Record<Event> record) {
        return record == null ? null : record.getData();
    }

    private static final class SerializedEvent {
        private final Event event;
        private final String message;
        private final byte[] bytes;

        private SerializedEvent(final Event event, final String message, final byte[] bytes) {
            this.event = event;
            this.message = message;
            this.bytes = bytes;
        }
    }

    @Override
    public boolean isReady() {
        return isInitialized;
    }

    /**
     * Logs custom headers configuration during plugin startup.
     * Ensures no sensitive header values are logged.
     *
     * @param headerOverrides The custom headers map to log
     */
    private void logCustomHeadersConfiguration(Map<String, String> headerOverrides) {
        if (LOG.isInfoEnabled()) {
            if (headerOverrides.isEmpty()) {
                LOG.info("CloudWatch Logs sink initialized without custom headers");
            } else {
                int headerCount = headerOverrides.size();
                String headerNames = String.join(", ", headerOverrides.keySet());

                LOG.info("CloudWatch Logs sink initialized with {} custom headers: [{}]",
                        headerCount, headerNames);
            }
        }
    }
}
