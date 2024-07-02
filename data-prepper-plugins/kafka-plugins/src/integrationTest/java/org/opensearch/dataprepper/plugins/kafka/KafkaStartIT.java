package org.opensearch.dataprepper.plugins.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static org.awaitility.Awaitility.await;

/**
 * This waits for Kafka to become available. It isn't a true integration test.
 */
public class KafkaStartIT {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStartIT.class);

    @Test
    void waitForKafka() {
        String bootstrapServers = System.getProperty("tests.kafka.bootstrap_servers");

        LOG.info("Using Kafka bootstrap servers: {}", bootstrapServers);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {
            await().atMost(Duration.ofMinutes(3))
                    .pollDelay(Duration.ofSeconds(2))
                    .until(() -> adminClient.listTopics().names().get() != null);
        }
    }
}
