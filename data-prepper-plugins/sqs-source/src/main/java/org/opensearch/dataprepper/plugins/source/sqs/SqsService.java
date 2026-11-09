/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

 package org.opensearch.dataprepper.plugins.source.sqs;

 import com.linecorp.armeria.client.retry.Backoff;
 import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
 import org.opensearch.dataprepper.metrics.PluginMetrics;
 import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
 import org.opensearch.dataprepper.plugins.source.sqs.common.SqsBackoff;
 import org.opensearch.dataprepper.plugins.source.sqs.common.SqsClientFactory;
 import org.opensearch.dataprepper.plugins.source.sqs.common.SqsWorkerCommon;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;
 import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
 import software.amazon.awssdk.regions.Region;
 import software.amazon.awssdk.services.sqs.SqsClient;
 import org.opensearch.dataprepper.model.buffer.Buffer;
 import org.opensearch.dataprepper.model.event.Event;
 import org.opensearch.dataprepper.model.record.Record;

 import java.util.ArrayList;
 import java.util.HashMap;
 import java.util.List;
 import java.util.Map;
 import java.time.Duration;
import java.util.concurrent.TimeUnit;
 import java.util.concurrent.Executors;
 import java.util.concurrent.ExecutorService;
 import java.util.stream.Collectors;
 import java.util.stream.IntStream;

 public class SqsService {
     private static final Logger LOG = LoggerFactory.getLogger(SqsService.class);
     static final long SHUTDOWN_TIMEOUT = 30L;
     // Reserve one third of the shutdown window to drain pending acknowledgement callbacks.
     static final long ACKNOWLEDGEMENT_SHUTDOWN_RESERVE = SHUTDOWN_TIMEOUT / 3;
     private final SqsSourceConfig sqsSourceConfig;
     private final PluginMetrics pluginMetrics;
     private final AcknowledgementSetManager acknowledgementSetManager;
     private final List<ExecutorService> allSqsUrlExecutorServices;
     private final List<SqsWorker> sqsWorkers;
     private final Buffer<Record<Event>> buffer;
     private final Map<String, SqsClient> sqsClientMap = new HashMap<>();
     private final AwsCredentialsProvider credentialsProvider;


     public SqsService(final Buffer<Record<Event>> buffer,
                       final AcknowledgementSetManager acknowledgementSetManager,
                       final SqsSourceConfig sqsSourceConfig,
                       final PluginMetrics pluginMetrics,
                       final AwsCredentialsProvider credentialsProvider) {

        this.sqsSourceConfig = sqsSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.credentialsProvider = credentialsProvider;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.allSqsUrlExecutorServices = new ArrayList<>();
        this.sqsWorkers = new ArrayList<>();
        this.buffer = buffer;
     }  

     public void start() {
        LOG.info("Starting SqsService");
        sqsSourceConfig.getQueues().forEach(queueConfig -> {
            String queueUrl = queueConfig.getUrl();
            String region = extractRegionFromQueueUrl(queueUrl);
            SqsClient sqsClient = sqsClientMap.computeIfAbsent(region,
                    r -> SqsClientFactory.createSqsClient(Region.of(r), credentialsProvider));
            String queueName = queueUrl.substring(queueUrl.lastIndexOf('/') + 1);
            Backoff backoff = SqsBackoff.createExponentialBackoff();
            SqsWorkerCommon sqsWorkerCommon = new SqsWorkerCommon(backoff, pluginMetrics, acknowledgementSetManager);
            int numWorkers = queueConfig.getNumWorkers();
            SqsEventProcessor sqsEventProcessor;
            MessageFieldStrategy strategy;
            if (queueConfig.getCodec() != null) {
                strategy = new CodecBulkMessageFieldStrategy(queueConfig.getCodec());
            } else {
                strategy = new StandardMessageFieldStrategy();
            }

            sqsEventProcessor = new SqsEventProcessor(new RawSqsMessageHandler(strategy));
            ExecutorService executorService = Executors.newFixedThreadPool(
                    numWorkers, BackgroundThreadFactory.defaultExecutorThreadFactory("sqs-source" + queueName));
            allSqsUrlExecutorServices.add(executorService);
            List<SqsWorker> workers = IntStream.range(0, numWorkers)
                    .mapToObj(i -> new SqsWorker(
                            buffer,
                            acknowledgementSetManager,
                            sqsClient,
                            sqsWorkerCommon,
                            sqsSourceConfig,
                            queueConfig,
                            pluginMetrics,
                            sqsEventProcessor))
                    .collect(Collectors.toList());

            sqsWorkers.addAll(workers); 
            workers.forEach(executorService::submit);
            LOG.info("Started SQS workers for queue {} with {} workers", queueUrl, numWorkers);
        });
    }


     public void stop() {
        allSqsUrlExecutorServices.forEach(ExecutorService::shutdown);
        sqsWorkers.forEach(SqsWorker::stop);
        final long executorShutdownDeadlineNanos = System.nanoTime() + getExecutorShutdownTimeoutNanos();
        try {
            for (final ExecutorService executorService : allSqsUrlExecutorServices) {
                final long remainingNanos = Math.max(0, executorShutdownDeadlineNanos - System.nanoTime());
                if (!executorService.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                    LOG.warn("Failed to terminate SqsWorkers before the shutdown timeout");
                    executorService.shutdownNow();
                }
            }

            final long acknowledgementShutdownDeadlineNanos = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(ACKNOWLEDGEMENT_SHUTDOWN_RESERVE);
            for (final SqsWorker sqsWorker : sqsWorkers) {
                final long remainingNanos = Math.max(0, acknowledgementShutdownDeadlineNanos - System.nanoTime());
                if (!sqsWorker.waitForPendingAcknowledgements(Duration.ofNanos(remainingNanos))) {
                    LOG.warn("Timed out waiting for pending SQS acknowledgments. Their messages may be redelivered.");
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted during shutdown, exiting uncleanly...", e);
            allSqsUrlExecutorServices.forEach(ExecutorService::shutdownNow);
            Thread.currentThread().interrupt();
        } finally {
            sqsWorkers.forEach(SqsWorker::markSqsClientClosed);
            sqsClientMap.values().forEach(SqsClient::close);
        }

        LOG.info("SqsService shutdown completed.");
    }

     static long getExecutorShutdownTimeoutNanos() {
        return TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT - ACKNOWLEDGEMENT_SHUTDOWN_RESERVE);
    }

     private String extractRegionFromQueueUrl(final String queueUrl) {
         String[] split = queueUrl.split("\\.");
         return split[1];
     }
    
 }
