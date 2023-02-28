package org.opensearch.dataprepper.plugins.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.consumer.MultithreadedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Event>> {

	private static final Logger logger = LoggerFactory.getLogger(KafkaSource.class);
	private final KafkaSourceConfig sourceConfig;
	final List<MultithreadedConsumer> consumers = new ArrayList<>();
	private ExecutorService executorService;

	@DataPrepperPluginConstructor
	public KafkaSource(final KafkaSourceConfig sourceConfig) {
		this.sourceConfig = sourceConfig;
	}

	@Override
	public void start(Buffer<Record<Event>> buffer) {
		int consumerCount = KafkaSourceConfig.CONSUMER_COUNT;
		String consumerGroups = KafkaSourceConfig.CONSUMER_GROUP_NAME;
		Properties props = getConsumerProperties();
		try {
			if (executorService == null || executorService.isShutdown()) {
				executorService = Executors.newFixedThreadPool(consumerCount);
				IntStream.range(0, consumerCount).forEach(index -> {
					String consumerId = Integer.toString(index + 1);
					MultithreadedConsumer kafkaSourceConsumer = new MultithreadedConsumer(consumerId, consumerGroups,
							props, sourceConfig, buffer);
					consumers.add(kafkaSourceConsumer);
					executorService.submit(kafkaSourceConsumer);
				});
			}
		} catch (Exception e) {
			logger.error("Exception while reading records from the Kafka topic...{}", e.getMessage());
		}

	}

	@Override
	public void stop() {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Shutting down Consumers...");
			for (MultithreadedConsumer kafkaConsumer : consumers) {
				kafkaConsumer.shutdownConsumer();
			}
			logger.info("Closing the Kafka Consumer Application...");
			executorService.shutdown();
			try {
				executorService.awaitTermination(KafkaSourceConfig.THREAD_WAITING_MILLI_SEC, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				if (e.getCause() instanceof InterruptedException) {
					Thread.currentThread().interrupt();
				}
				throw new RuntimeException(e);
			}
		}));
	}

	private Properties getConsumerProperties() {
		Properties props = new Properties();
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, sourceConfig.getEnableAutoCommitInterval());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, sourceConfig.getAutoOffsetReset());
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootStrapServers());
		// props.put(ConsumerConfig.CLIENT_ID_CONFIG, "");
		// props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, sourceConfig.getEnableAutoCommit());
		// props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "");
		// props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 10485880L);
		// props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "");
		// props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, sourceConfig.getGroupId());
		// props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, sourceConfig.getKeyDeserializer());
		// props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "");
		// props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "");
		// props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "");
		// props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "");
		// props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "");
		// props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "");
		// props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "");
		// props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "");
		// props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "");
		// props.put(ConsumerConfig.SEND_BUFFER_CONFIG, "");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sourceConfig.getSessionTimeOut());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, sourceConfig.getValueDeserializer());

		return props;
	}
}
