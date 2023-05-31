/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.aws.sqs.common.BufferAccumulator;
import org.opensearch.dataprepper.plugins.aws.sqs.common.codec.Codec;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsSourceTask;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SqsSourceService {

    private final SqsSourceConfig sqsSourceConfig;

    private final AcknowledgementSetManager acknowledgementSetManager;

    private final SqsMetrics sqsMetrics;

    private final ExecutorService executor;

    private final BufferAccumulator<Record<Event>> bufferAccumulator;

    private final Codec codec;

    public SqsSourceService(final SqsSourceConfig sqsSourceConfig,
                            final AcknowledgementSetManager acknowledgementSetManager,
                            final SqsMetrics sqsMetrics,
                            final BufferAccumulator<Record<Event>> bufferAccumulator,
                            final Codec codec) {
        this.sqsSourceConfig = sqsSourceConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sqsMetrics = sqsMetrics;
        this.executor = Executors.newFixedThreadPool(sqsSourceConfig.getQueues().getNumberOfThreads());
        this.bufferAccumulator = bufferAccumulator;
        this.codec = codec;
    }

    public void processSqsMessages() {
        SqsService sqsService = new SqsService(sqsMetrics,
                sqsSourceConfig.getAws().getAwsRegion(),
                sqsSourceConfig.getAws().authenticateAwsConfiguration()
                ,bufferAccumulator,codec);

        SqsOptions.Builder sqsOptionsBuilder = new SqsOptions.Builder()
                .setPollDelay(sqsSourceConfig.getQueues().getPollingFrequency())
                .setMaximumMessages(sqsSourceConfig.getQueues().getBatchSize());

        sqsSourceConfig.getQueues().getUrls().forEach(url ->
            executor.execute(new SqsSourceTask(sqsService,
                    sqsOptionsBuilder.setSqsUrl(url).build(),
                    sqsMetrics,
                    acknowledgementSetManager,
                    sqsSourceConfig.getAcknowledgements())));
    }

    public void stop(){
        executor.shutdown();
    }

}
