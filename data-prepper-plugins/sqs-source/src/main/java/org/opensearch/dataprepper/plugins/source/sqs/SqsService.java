/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

 package org.opensearch.dataprepper.plugins.source.sqs;

 import com.linecorp.armeria.client.retry.Backoff;
 import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
 import org.opensearch.dataprepper.metrics.PluginMetrics;
 import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
 import org.opensearch.dataprepper.model.codec.InputCodec;
 import org.opensearch.dataprepper.model.configuration.PluginModel;
 import org.opensearch.dataprepper.model.configuration.PluginSetting;
 import org.opensearch.dataprepper.model.plugin.PluginFactory;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;
 import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
 import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
 import software.amazon.awssdk.core.retry.RetryPolicy;
 import software.amazon.awssdk.services.sqs.SqsClient;
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
     private final SqsClient sqsClient;
     private final PluginMetrics pluginMetrics;
     private final PluginFactory pluginFactory;
     private final AcknowledgementSetManager acknowledgementSetManager;
     private final List<ExecutorService> allSqsUrlExecutorServices;
     private final List<SqsWorker> sqsWorkers;
     private final Buffer<Record<Event>> buffer;

     public SqsService(final Buffer<Record<Event>> buffer,
                       final AcknowledgementSetManager acknowledgementSetManager,
                       final SqsSourceConfig sqsSourceConfig,
                       final PluginMetrics pluginMetrics,
                       final PluginFactory pluginFactory,
                       final AwsCredentialsProvider credentialsProvider) {
                        
        this.sqsSourceConfig = sqsSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pluginFactory = pluginFactory;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.allSqsUrlExecutorServices = new ArrayList<>();
        this.sqsWorkers = new ArrayList<>();
        this.sqsClient = createSqsClient(credentialsProvider); 
        this.buffer = buffer;
     }  
 

     public void start() {
        final Backoff backoff = Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY).withJitter(JITTER_RATE)
                 .withMaxAttempts(Integer.MAX_VALUE);
                 
        LOG.info("Starting SqsService");

        sqsSourceConfig.getQueues().forEach(queueConfig -> {
            String queueUrl = queueConfig.getUrl();
            String queueName = queueUrl.substring(queueUrl.lastIndexOf('/') + 1);
            int numWorkers = queueConfig.getNumWorkers();
            SqsEventProcessor sqsEventProcessor;
            if (queueConfig.getCodec() != null) {
                final PluginModel codecConfiguration = queueConfig.getCodec();
                final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
                final InputCodec codec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSettings);
                MessageFieldStrategy bulkStrategy = new JsonBulkMessageFieldStrategy(codec);
                sqsEventProcessor = new SqsEventProcessor(new RawSqsMessageHandler(bulkStrategy));
            } else {
                MessageFieldStrategy standardStrategy = new StandardMessageFieldStrategy();
                sqsEventProcessor = new SqsEventProcessor(new RawSqsMessageHandler(standardStrategy));
            }
            ExecutorService executorService = Executors.newFixedThreadPool(
                    numWorkers, BackgroundThreadFactory.defaultExecutorThreadFactory("sqs-source" + queueName));
            allSqsUrlExecutorServices.add(executorService);
            List<SqsWorker> workers = IntStream.range(0, numWorkers)
                    .mapToObj(i -> new SqsWorker(
                            buffer,
                            acknowledgementSetManager,
                            sqsClient,
                            sqsSourceConfig,
                            queueConfig,
                            pluginMetrics,
                            sqsEventProcessor,
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
 