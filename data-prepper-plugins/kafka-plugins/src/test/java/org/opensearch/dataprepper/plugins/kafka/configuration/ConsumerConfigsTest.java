package org.opensearch.dataprepper.plugins.kafka.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mock;
import org.yaml.snakeyaml.Yaml;

class ConsumerConfigsTest {

	@Mock
	ConsumerConfigs consumerConfigs;

	private static final String YAML_FILE_WITH_CONSUMER_CONFIG = "sample-pipelines.yaml";

	private static final String YAML_FILE_WITH_MISSING_CONSUMER_CONFIG = "sample-pipelines-1.yaml";

	private static final String AUTO_COMMIT = "false";
	private static final Duration AUTOCOMMIT_INTERVAL = Duration.ofSeconds(5);
	private static final Duration SESSION_TIMEOUT = Duration.ofSeconds(45);
	private static final int MAX_RETRY_ATTEMPT = 1000;
	private static final String AUTO_OFFSET_RESET = "earliest";
	private static final Duration THREAD_WAITING_TIME = Duration.ofSeconds(1);
	private static final Duration MAX_RECORD_FETCH_TIME = Duration.ofSeconds(4);
	private static final Duration BUFFER_DEFAULT_TIMEOUT = Duration.ofSeconds(5);
	private static final Long FETCH_MAX_BYTES = 52428800L;
	private static final Long FETCH_MAX_WAIT = 500L;
	private static final Long FETCH_MIN_BYTES = 1L;
	private static final Duration RETRY_BACKOFF = Duration.ofSeconds(100);
	private static final Duration MAX_POLL_INTERVAL = Duration.ofSeconds(300000);
	private static final Long CONSUMER_MAX_POLL_RECORDS = 500L;
	private static final Integer NUM_OF_WORKERS = 10;
	private static final Duration HEART_BEAT_INTERVAL_DURATION = Duration.ofSeconds(3);


	@BeforeEach
	void setUp(TestInfo testInfo) throws IOException {
		String fileName = testInfo.getTags().stream().findFirst().orElse("");
		consumerConfigs = new ConsumerConfigs();
		//Added to load Yaml file - Start
		Yaml yaml = new Yaml();
		FileReader reader = new FileReader(getClass().getClassLoader().getResource(fileName).getFile());
		Object data = yaml.load(reader);
		ObjectMapper mapper = new ObjectMapper();
		if(data instanceof Map){
			Map<Object, Object> propertyMap = (Map<Object, Object>) data;
			Map<Object, Object> nestedMap = new HashMap<>();
			iterateMap(propertyMap, nestedMap);
			List<TopicConfig> topicConfigList = (List<TopicConfig>) nestedMap.get("topics");
			mapper.registerModule(new JavaTimeModule());
			TopicsConfig topicsConfig = mapper.convertValue(topicConfigList.get(0), TopicsConfig.class);
			consumerConfigs = topicsConfig.getTopic().getConsumerGroupConfig();
		}
	}

	private static Map<Object, Object> iterateMap(Map<Object, Object> map, Map<Object, Object> nestedMap) {
		for (Map.Entry<Object, Object> entry : map.entrySet()) {
			String key = (String) entry.getKey();
			Object value = entry.getValue();
			if (value instanceof Map) {
				iterateMap((Map<Object, Object>) value, nestedMap);
			} else {
				nestedMap.put(key, value);
			}
		}
		return nestedMap;
	}

	@Test
	@Tag(YAML_FILE_WITH_MISSING_CONSUMER_CONFIG)
	void testConfigValues_default() {
		assertEquals(AUTO_COMMIT, consumerConfigs.getAutoCommit());
		assertEquals(AUTOCOMMIT_INTERVAL, consumerConfigs.getAutoCommitInterval());
		assertEquals(SESSION_TIMEOUT, consumerConfigs.getSessionTimeOut());
		assertEquals(AUTO_OFFSET_RESET, consumerConfigs.getAutoOffsetReset());
		assertEquals(THREAD_WAITING_TIME, consumerConfigs.getThreadWaitingTime());
		assertEquals(MAX_RECORD_FETCH_TIME, consumerConfigs.getMaxRecordFetchTime());
		assertEquals(BUFFER_DEFAULT_TIMEOUT, consumerConfigs.getBufferDefaultTimeout());
		assertEquals(FETCH_MAX_BYTES, consumerConfigs.getFetchMaxBytes());
		assertEquals(FETCH_MAX_WAIT, consumerConfigs.getFetchMaxWait());
		assertEquals(FETCH_MIN_BYTES, consumerConfigs.getFetchMinBytes());
		assertEquals(RETRY_BACKOFF, consumerConfigs.getRetryBackoff());
		assertEquals(MAX_POLL_INTERVAL, consumerConfigs.getMaxPollInterval());
		assertEquals(CONSUMER_MAX_POLL_RECORDS, consumerConfigs.getConsumerMaxPollRecords());
		assertEquals(NUM_OF_WORKERS, consumerConfigs.getWorkers());
		assertEquals(HEART_BEAT_INTERVAL_DURATION, consumerConfigs.getHeartBeatInterval());
	}

	@Test
	@Tag(YAML_FILE_WITH_CONSUMER_CONFIG)
	void testConfigValues_from_yaml() {

		assertEquals(AUTO_COMMIT, consumerConfigs.getAutoCommit());
		assertEquals(AUTOCOMMIT_INTERVAL, consumerConfigs.getAutoCommitInterval());
		assertEquals(SESSION_TIMEOUT, consumerConfigs.getSessionTimeOut());
		assertEquals(AUTO_OFFSET_RESET, consumerConfigs.getAutoOffsetReset());
		assertEquals(THREAD_WAITING_TIME, consumerConfigs.getThreadWaitingTime());
		assertEquals(MAX_RECORD_FETCH_TIME, consumerConfigs.getMaxRecordFetchTime());
		assertEquals(BUFFER_DEFAULT_TIMEOUT, consumerConfigs.getBufferDefaultTimeout());
		assertEquals(FETCH_MAX_BYTES, consumerConfigs.getFetchMaxBytes());
		assertEquals(FETCH_MAX_WAIT, consumerConfigs.getFetchMaxWait());
		assertEquals(FETCH_MIN_BYTES, consumerConfigs.getFetchMinBytes());
		assertEquals(RETRY_BACKOFF, consumerConfigs.getRetryBackoff());
		assertEquals(MAX_POLL_INTERVAL, consumerConfigs.getMaxPollInterval());
		assertEquals(CONSUMER_MAX_POLL_RECORDS, consumerConfigs.getConsumerMaxPollRecords());
		assertEquals(NUM_OF_WORKERS, consumerConfigs.getWorkers());
		assertEquals(HEART_BEAT_INTERVAL_DURATION, consumerConfigs.getHeartBeatInterval());
	}

	@Test
	@Tag(YAML_FILE_WITH_CONSUMER_CONFIG)
	void testConfigValues_from_yaml_not_null() {

		assertNotNull(consumerConfigs.getAutoCommit());
		assertNotNull(consumerConfigs.getAutoCommitInterval());
		assertNotNull(consumerConfigs.getSessionTimeOut());
		assertNotNull(consumerConfigs.getAutoOffsetReset());
		assertNotNull(consumerConfigs.getThreadWaitingTime());
		assertNotNull(consumerConfigs.getMaxRecordFetchTime());
		assertNotNull(consumerConfigs.getBufferDefaultTimeout());
		assertNotNull(consumerConfigs.getFetchMaxBytes());
		assertNotNull(consumerConfigs.getFetchMaxWait());
		assertNotNull(consumerConfigs.getFetchMinBytes());
		assertNotNull(consumerConfigs.getRetryBackoff());
		assertNotNull(consumerConfigs.getMaxPollInterval());
		assertNotNull(consumerConfigs.getConsumerMaxPollRecords());
		assertNotNull(consumerConfigs.getWorkers());
		assertNotNull(consumerConfigs.getHeartBeatInterval());
	}
}
