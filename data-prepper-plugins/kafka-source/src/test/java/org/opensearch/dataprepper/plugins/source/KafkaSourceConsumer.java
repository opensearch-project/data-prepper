package org.opensearch.dataprepper.plugins.source;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;



public class KafkaSourceConsumer {
	
	private static AtomicInteger msg_counter = new AtomicInteger(0);
	private final KafkaConsumer<String, String> consumer;
	private static final Logger logger = LoggerFactory.getLogger(KafkaSourceConsumer.class);
	
	public KafkaSourceConsumer(String consumerGroupId) {
		Properties consumerProps = ConsumerConfigs.getConfigs(consumerGroupId);
		consumer = new KafkaConsumer<>(consumerProps);	
	}

	
	public void readMessageFromTopic(String consumerId, String consumerGroupId) {
		 System.out.printf("Start reading consumer: %s, group: %s%n", consumerId, consumerGroupId);
	      
	     // KafkaConsumer consumer = new KafkaConsumer(consumerProps);
	      consumer.subscribe(Collections.singleton(ConsumerConfigs.TOPIC_NAME));
	      while (true) {
	          ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
	          for (ConsumerRecord<String, String> record : records) {
	              msg_counter.incrementAndGet();
	              System.out.printf("consumer id:%s, partition id= %s, key = %s, value = %s"
	                              + ", offset = %s%n",
	                      consumerId, record.partition(),record.key(), record.value(), record.offset());
	          }
	          
	          if (records.isEmpty()) {
	             logger.info("Last polling record is empty...");
	              break;
	          }

	          consumer.commitSync();
	          logger.info("Total number of processed messges :"+msg_counter.get());
	          
	      }
		
	}
	
	
	//testing purpose
	public void showTopicDestails(String topicName) {
		Properties config = new Properties();
	    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfigs.BOOT_STRAP_SERVER);
	    AdminClient adminClient = AdminClient.create(config);
	    
	    System.out.println("------------> Topic Details <-----------------");
	    try {
			adminClient.describeTopics(Collections.singleton(topicName)).all().get()
			       .forEach((topic, desc) -> {
			           System.out.println("Topic: " + topic);
			           System.out.printf("Partitions: %s, partition ids: %s%n", desc.partitions().size(),
			                   desc.partitions()
			                       .stream()
			                       .map(p -> Integer.toString(p.partition()))
			                       .collect(Collectors.joining(",")));
			       });
		} catch (InterruptedException | ExecutionException e) {	
			logger.error("Error while reading the Topic details : "+e.getMessage());
		}finally {
	    adminClient.close();
		}
		
	}

}








