package org.opensearch.dataprepper.plugins.kafkasource;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Event>>{

		//private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
	static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
	static final String GROUP_ID = "group.id";
	static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
	static final String ENABLE_AUTO_COMMIT_INTERVAL="auto.commit.interval.ms";
	static final String SESSION_TIMEOUT="session.timeout.ms";
	static final String KEY_DESERIALIZER="key.deserializer";
	static final String VALUE_DESERIALIZER="value.deserializer";
	
	 private final KafkaSourceConfig sourceConfig;
			// TODO Auto-generated method stub

	    @DataPrepperPluginConstructor
	    public KafkaSource( final KafkaSourceConfig sourceConfig) {
	        
	        this.sourceConfig = sourceConfig;
	    }

		@Override
		public void start(Buffer<Record<Event>> buffer) {
		      //Kafka consumer configuration settings
		      String topicName = "Ammoraiah123";
		      Properties props = new Properties();
		      
		      props.put(BOOTSTRAP_SERVERS, sourceConfig.getBootstrapservers());
		      props.put(GROUP_ID, sourceConfig.getGroupId());
		      props.put(ENABLE_AUTO_COMMIT, sourceConfig.getEnableautocommit());
		      props.put(ENABLE_AUTO_COMMIT_INTERVAL, sourceConfig.getEnableautocommitinterval());
		      props.put(SESSION_TIMEOUT, sourceConfig.getSessiontimeout());
		      props.put(KEY_DESERIALIZER, 
		    		  sourceConfig.getKeyDeserializer());
		      props.put(VALUE_DESERIALIZER, 
		    		  sourceConfig.getValueDeserializer());
		      
		      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		      
		      
		      //Kafka Consumer subscribes list of topics here.
		      consumer.subscribe(Arrays.asList(topicName));
		      
		      //print the topic name
		      System.out.println("Subscribed to topic " + topicName);
		      int i = 0;
		      
		      while (true) {
		         ConsumerRecords<String, String> records = consumer.poll(100);
		         for (ConsumerRecord<String, String> record : records)
		         
		         // print the offset,key and value for the consumer records.
		         System.out.printf("offset = %d, key = %s, value = %s\n", 
		            record.offset(), record.key(), record.value());
		      }
			
		}

		@Override
		public void stop() {
			// TODO Auto-generated method stub
			
		}

		
		
}
