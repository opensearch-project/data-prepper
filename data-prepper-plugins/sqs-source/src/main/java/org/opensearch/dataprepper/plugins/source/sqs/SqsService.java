/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

 package org.opensearch.dataprepper.plugins.source.sqs;

 import com.linecorp.armeria.client.retry.Backoff;
 import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
 import org.opensearch.dataprepper.metrics.PluginMetrics;
 import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;
 import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
 import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
 import software.amazon.awssdk.core.retry.RetryPolicy;
 import software.amazon.awssdk.services.sqs.SqsClient;
 import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
 import org.opensearch.dataprepper.model.buffer.Buffer;
 import org.opensearch.dataprepper.model.event.Event;
 import org.opensearch.dataprepper.model.record.Record;
 import java.time.Duration;
 import java.util.ArrayList;
 import java.util.List;
 import java.util.concurrent.TimeUnit;
 import java.util.concurrent.Executors;
 import java.util.concurrent.ExecutorService;
 import java.util.stream.Collectors;
 import java.util.stream.IntStream;
 
 public class SqsService {
     private static final Logger LOG = LoggerFactory.getLogger(SqsService.class);
     static final long SHUTDOWN_TIMEOUT = 30L;
     static final long INITIAL_DELAY = Duration.ofSeconds(20).toMillis();
     static final long MAXIMUM_DELAY = Duration.ofMinutes(5).toMillis();
     static final double JITTER_RATE = 0.20;
    
     private final SqsSourceConfig sqsSourceConfig;
     private final SqsEventProcessor sqsEventProcessor;
     private final SqsClient sqsClient;
     private final PluginMetrics pluginMetrics;
     private final AcknowledgementSetManager acknowledgementSetManager;
     private final List<ExecutorService> allSqsUrlExecutorServices;
     private final List<SqsWorker> sqsWorkers;
     private final BufferAccumulator<Record<Event>> bufferAccumulator;

     public SqsService(final Buffer<Record<Event>> buffer,
                       final AcknowledgementSetManager acknowledgementSetManager,
                       final SqsSourceConfig sqsSourceConfig,
                       final SqsEventProcessor sqsEventProcessor,
                       final PluginMetrics pluginMetrics,
                       final AwsCredentialsProvider credentialsProvider) {
                        
        this.sqsSourceConfig = sqsSourceConfig;
        this.sqsEventProcessor = sqsEventProcessor;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.allSqsUrlExecutorServices = new ArrayList<>();
        this.sqsWorkers = new ArrayList<>();
        this.sqsClient = createSqsClient(credentialsProvider); 
        this.bufferAccumulator = BufferAccumulator.create(buffer, sqsSourceConfig.getNumberOfRecordsToAccumulate(), sqsSourceConfig.getBufferTimeout());

     }  
 

     public void start() {
        final Backoff backoff = Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY).withJitter(JITTER_RATE)
                 .withMaxAttempts(Integer.MAX_VALUE);
                 
        LOG.info("Starting SqsService");

        sqsSourceConfig.getQueues().forEach(queueConfig -> {
            String queueUrl = queueConfig.getUrl();
            int numWorkers = queueConfig.getNumWorkers();
            ExecutorService executorService = Executors.newFixedThreadPool(
                    numWorkers, BackgroundThreadFactory.defaultExecutorThreadFactory("sqs-source-new-" + queueUrl));
            allSqsUrlExecutorServices.add(executorService);
            List<SqsWorker> workers = IntStream.range(0, numWorkers)
                    .mapToObj(i -> new SqsWorker(
                            bufferAccumulator,
                            acknowledgementSetManager,
                            sqsClient,
                            sqsEventProcessor,
                            sqsSourceConfig,
                            queueConfig,
                            pluginMetrics,
                            backoff))
                    .collect(Collectors.toList());

            sqsWorkers.addAll(workers); 
            workers.forEach(executorService::submit);
            LOG.info("Started SQS workers for queue {} with {} workers", queueUrl, numWorkers);
        });
    }
 
     SqsClient createSqsClient(final AwsCredentialsProvider credentialsProvider) {
         LOG.debug("Creating SQS client");
         return SqsClient.builder()
                 .region(sqsSourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                 .credentialsProvider(credentialsProvider)
                 .overrideConfiguration(ClientOverrideConfiguration.builder()
                         .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                         .build())
                 .build();
     }

     public void stop() {
        allSqsUrlExecutorServices.forEach(ExecutorService::shutdown);
        sqsWorkers.forEach(SqsWorker::stop);
        allSqsUrlExecutorServices.forEach(executorService -> {
            try {
                if (!executorService.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
                    LOG.warn("Failed to terminate SqsWorkers");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOG.error("Interrupted during shutdown, exiting uncleanly...", e);
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        });
    
        sqsClient.close();
        LOG.info("SqsService shutdown completed.");
    }
    
 }
 