package org.opensearch.dataprepper.plugins.source.sqs;

import org.junit.jupiter.api.Test;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class QueueConfigTest {

    @Test
    void testDefaultValues() {
        final QueueConfig queueConfig = new QueueConfig();

        assertNull(queueConfig.getUrl(), "URL should be null by default");
        assertEquals(1, queueConfig.getNumWorkers(), "Number of workers should default to 1");
        assertNull(queueConfig.getMaximumMessages(), "Maximum messages should be null by default");
        assertEquals(Duration.ofSeconds(0), queueConfig.getPollDelay(), "Poll delay should default to 0 seconds");
        assertNull(queueConfig.getVisibilityTimeout(), "Visibility timeout should be null by default");
        assertFalse(queueConfig.getVisibilityDuplicateProtection(), "Visibility duplicate protection should default to false");
        assertEquals(Duration.ofHours(2), queueConfig.getVisibilityDuplicateProtectionTimeout(),
                "Visibility duplicate protection timeout should default to 2 hours");
        assertNull(queueConfig.getWaitTime(), "Wait time should default to null");
    }
}