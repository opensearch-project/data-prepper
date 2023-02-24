package org.opensearch.dataprepper.plugins.source;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

public class MultithreadedKafkaConsumer implements Runnable, ConsumerRebalanceListener {

	private static AtomicInteger record_count = new AtomicInteger(0);
	private final KafkaConsumer<String, String> consumer;
	private static final Logger logger = LoggerFactory.getLogger(MultithreadedKafkaConsumer.class);
	private final AtomicBoolean status = new AtomicBoolean(false);
	private long lastCommitTime = System.currentTimeMillis();
	private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

	private final List<String> topics;
	private String consumerId;
	private String consumerGroupId;
	private Properties consumerProps;

	public MultithreadedKafkaConsumer(String consumerId, String consumerGroupId, List<String> topics,Properties props) {
		this.consumerProps = props;
		this.consumer = new KafkaConsumer<>(consumerProps);
		this.topics = topics;
		this.consumerId = consumerId;
		this.consumerGroupId = consumerGroupId;
	}

	@Override
	public void run() throws WakeupException {
		System.out.printf("Start reading consumer: %s, group: %s%n", consumerId, consumerGroupId);
		try {
			if (!topics.isEmpty()) {
				consumer.subscribe(topics, this);
				while (!status.get()) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					if (records.count() > 0 ) {
						for (TopicPartition partition : records.partitions()) {
							List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
							for (ConsumerRecord<String, String> record : partitionRecords) {
								System.out.println("\n================" + record.offset() + ": " + record.value());
								// collect offsets for revoked partitions
								currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
										new OffsetAndMetadata(record.offset() + 1, null));
							}
							long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
							commitOffsets(partition, lastOffset, consumer);
							record_count.incrementAndGet();
						}
						logger.info("Total number of records processed :" + record_count.get());
					}
				}
			} else {
				logger.info("The given TOPIC name is empty and couldn't be able to read the records...");
			}
		} catch (Exception we) {
			if (!status.get())
				logger.error("Error reading records from the topic..." + we.getMessage());
			throw new RuntimeException(we);
		} finally {
			logger.info("Closing the consumer... " + consumerId);
			consumer.close();
		}
	}

	
	
	private void commitOffsets(TopicPartition partition, long lastOffset, KafkaConsumer<String, String> consumer) {
		try {
			long currentTimeMillis = System.currentTimeMillis();
			if (currentTimeMillis - lastCommitTime > 4000) {
				consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
			}
			lastCommitTime = currentTimeMillis;

		} catch (CommitFailedException e) {
			// TODO NEED TO IMPLEMENT THE RETRY LOGIC HERE
			logger.error(KafkaConstants.COMMIT_FAILURE_MSG,e);
			if(e.getCause() instanceof Exception) {}
			
		}
	}

	public void shutdownConsumer() {
		status.set(false);
		consumer.wakeup();
	}
	
	private boolean checkTopicsAvailability(List<String> topics) {
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		AdminClient adminClient = AdminClient.create(config);
		try {
			boolean topicAvailability = adminClient.listTopics().names().get().stream()
					.anyMatch(topicName -> topicName.equalsIgnoreCase(KafkaSourceConfig.TOPIC_NAME)); 
			return topicAvailability;
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Error while checking the given topic name : " + e.getMessage());
		} finally {
			adminClient.close();
		}
		return false;
	}
	
	
	public void showTopicDestails(String topicName) {
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		AdminClient adminClient = AdminClient.create(config);

		System.out.println("------------> Topic Details <-----------------");
		try {
			adminClient.describeTopics(Collections.singleton(topicName)).all().get().forEach((topic, desc) -> {
				System.out.println("Topic: " + topic);
				System.out.printf("Partitions: %s, partition ids: %s%n", desc.partitions().size(), desc.partitions()
						.stream().map(p -> Integer.toString(p.partition())).collect(Collectors.joining(",")));
			});
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Error while reading the Topic details : " + e.getMessage());
		} finally {
			adminClient.close();
		}

	}

	

	//Testing purpose
	public void readMessageFromTopic(String consumerId, String consumerGroupId) {
		System.out.printf("Start reading consumer: %s, group: %s%n", consumerId, consumerGroupId);

		// KafkaConsumer consumer = new KafkaConsumer(consumerProps);
		consumer.subscribe(Collections.singleton(KafkaSourceConfig.TOPIC_NAME));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
			for (ConsumerRecord<String, String> record : records) {
				record_count.incrementAndGet();
				System.out.printf("consumer id:%s, partition id= %s, key = %s, value = %s" + ", offset = %s%n",
						consumerId, record.partition(), record.key(), record.value(), record.offset());
			}

			if (records.isEmpty()) {
				logger.info("Last polling record is empty...");
				break;
			}

			consumer.commitSync();
			logger.info("Total number of processed messges :" + record_count.get());

		}

	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		logger.info("onPartitionsAssigned() callback triggered...");
        logger.info("Closing the consumer...");
        consumer.resume(partitions);
		
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> arg0) {
		logger.info("onPartitionsRevoked() callback triggered...");
		logger.info("Committing the offsets: " + currentOffsets);
		//commit offsets for revoked partitions
		try {
			consumer.commitSync(currentOffsets);
		} catch (CommitFailedException e) {
			logger.error(KafkaConstants.COMMIT_FAILURE_MSG,e);
			// TODO NEED TO IMPLEMENT THE RETRY LOGIC HERE
		}

	}

}
