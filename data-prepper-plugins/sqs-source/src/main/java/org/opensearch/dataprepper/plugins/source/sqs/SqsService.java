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
 import org.opensearch.dataprepper.plugins.source.sqs.common.SqsBackoff;
 import org.opensearch.dataprepper.plugins.source.sqs.common.SqsClientFactory;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;
 import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
 import software.amazon.awssdk.services.sqs.SqsClient;
 import org.opensearch.dataprepper.model.buffer.Buffer;
 import org.opensearch.dataprepper.model.event.Event;
 import org.opensearch.dataprepper.model.record.Record;

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
     private final SqsSourceConfig sqsSourceConfig;
     private final SqsClient sqsClient;
     private final PluginMetrics pluginMetrics;
     private final PluginFactory pluginFactory;
     private final AcknowledgementSetManager acknowledgementSetManager;
     private final List<ExecutorService> allSqsUrlExecutorServices;
     private final List<SqsWorker> sqsWorkers;
     private final Buffer<Record<Event>> buffer;
     private final Backoff backoff;

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
        this.sqsClient = SqsClientFactory.createSqsClient(sqsSourceConfig.getAwsAuthenticationOptions().getAwsRegion(), credentialsProvider);
        this.buffer = buffer;
        backoff = SqsBackoff.createExponentialBackoff();
     }  

     public void start() {
        LOG.info("Starting SqsService");
        sqsSourceConfig.getQueues().forEach(queueConfig -> {
            String queueUrl = queueConfig.getUrl();
            String queueName = queueUrl.substring(queueUrl.lastIndexOf('/') + 1);
            int numWorkers = queueConfig.getNumWorkers();
            SqsEventProcessor sqsEventProcessor;
            MessageFieldStrategy strategy;
            if (queueConfig.getCodec() != null) {
                final PluginModel codecConfiguration = queueConfig.getCodec();
                final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
                final InputCodec codec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSettings);
                strategy = new CodecBulkMessageFieldStrategy(codec);
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
 