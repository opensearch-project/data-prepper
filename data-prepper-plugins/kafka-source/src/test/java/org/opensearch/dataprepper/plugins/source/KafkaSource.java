package org.opensearch.dataprepper.plugins.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
//import org.opensearch.dataprepper.plugins.config.ConsumerConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Event>> {

	private static final Logger logger = LoggerFactory.getLogger(KafkaSource.class);
	private final KafkaSourceConfig sourceConfig;

	@DataPrepperPluginConstructor
	public KafkaSource(final KafkaSourceConfig sourceConfig) {
		this.sourceConfig = sourceConfig;
	}

	@Override
	public void start(Buffer<Record<Event>> buffer) {
		final List<MultithreadedKafkaConsumer> consumers = new ArrayList<>();
		int consumerCount = KafkaSourceConfig.CONSUMER_COUNT;
		String consumerGroups = KafkaSourceConfig.CONSUMER_GROUP_NAME;
		Properties props = getProperties();
		try {
			ExecutorService executorService = Executors.newFixedThreadPool(consumerCount);
			for (int i = 0; i < consumerCount; i++) {
				String consumerId = Integer.toString(i + 1);
				MultithreadedKafkaConsumer kafkaSourceConsumer = new MultithreadedKafkaConsumer(consumerId,
						consumerGroups, Arrays.asList(KafkaSourceConfig.TOPIC_NAME), props);
				consumers.add(kafkaSourceConsumer);
				executorService.submit(kafkaSourceConsumer);
			}

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				logger.info("Shutting down Consumers...");
				for (MultithreadedKafkaConsumer kafkaConsumer : consumers) {
					kafkaConsumer.shutdownConsumer();
				}
				logger.info("Closing the Kafka Consumer Application...");
				executorService.shutdown();
				try {
					executorService.awaitTermination(KafkaSourceConfig.THREAD_WAITING_MILLI_SEC, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}));
		} catch (Exception e) {
			logger.error("Exception while reading records from the Kafka topic..." + e.getMessage());
		}

	}

	private Properties getProperties() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootstrapservers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, sourceConfig.getGroupId());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, sourceConfig.getEnableautocommit());
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, sourceConfig.getEnableautocommitinterval());
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sourceConfig.getSessiontimeout());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, sourceConfig.getKeyDeserializer());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, sourceConfig.getValueDeserializer());
		

		return props;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

}
