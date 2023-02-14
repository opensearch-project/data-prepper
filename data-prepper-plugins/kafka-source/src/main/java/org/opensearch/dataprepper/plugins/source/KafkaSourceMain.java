package com.kafka.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSourceMain {
	private static final Logger logger = LoggerFactory.getLogger(KafkaSourceMain.class);

	public static void main(String args[]) {

		int consumerCount = ConsumerConfigs.CONSUMER_COUNT;
		String consumerGroups = "test-consumer-group";
		KafkaSourceConsumer kafkaSourceConsumer = new KafkaSourceConsumer(consumerGroups);
		try {
			ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
			for (int i = 0; i < consumerCount; i++) {
				String consumerId = Integer.toString(i + 1);
				executorService.execute(() -> kafkaSourceConsumer.readMessageFromTopic(consumerId, consumerGroups));
			}
			executorService.shutdown();

			executorService.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.error("Excpetion while reading the Kafka Consumer" + e.getMessage());
		}
	}
}
