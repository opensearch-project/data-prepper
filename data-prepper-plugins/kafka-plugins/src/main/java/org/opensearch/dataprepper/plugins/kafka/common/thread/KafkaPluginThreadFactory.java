/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.common.thread;

import org.opensearch.dataprepper.plugins.kafka.common.KafkaMdc;
import org.slf4j.MDC;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of {@link ThreadFactory} for Kafka plugin threads.
 */
public class KafkaPluginThreadFactory implements ThreadFactory {
    private final ThreadFactory delegateThreadFactory;
    private final String threadPrefix;
    private final String kafkaPluginType;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    KafkaPluginThreadFactory(
            final ThreadFactory delegateThreadFactory,
            final String kafkaPluginType) {
        this.delegateThreadFactory = delegateThreadFactory;
        this.threadPrefix = "kafka-" + kafkaPluginType + "-";
        this.kafkaPluginType = kafkaPluginType;
    }

    /**
     * Creates an instance specifically for use with {@link Executors}.
     *
     * @param kafkaPluginType The name of the plugin type. e.g. sink, source, buffer
     * @return An instance of the {@link KafkaPluginThreadFactory}.
     */
    public static KafkaPluginThreadFactory defaultExecutorThreadFactory(final String kafkaPluginType) {
        return new KafkaPluginThreadFactory(Executors.defaultThreadFactory(), kafkaPluginType);
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        final Thread thread = delegateThreadFactory.newThread(() -> {
            MDC.put(KafkaMdc.MDC_KAFKA_PLUGIN_KEY, kafkaPluginType);
            runnable.run();
        });

        thread.setName(threadPrefix + threadNumber.getAndIncrement());

        return thread;
    }
}
