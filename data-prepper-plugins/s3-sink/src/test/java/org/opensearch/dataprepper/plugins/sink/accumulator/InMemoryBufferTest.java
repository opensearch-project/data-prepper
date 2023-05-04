package org.opensearch.dataprepper.plugins.sink.accumulator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import software.amazon.awssdk.services.s3.S3Client;
import java.io.IOException;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ExtendWith(MockitoExtension.class)
class InMemoryBufferTest {

    @Mock
    private S3Client s3Client;
    private InMemoryBuffer inMemoryBuffer;

    @Test
    void test_with_write_event_into_buffer() throws IOException {
        inMemoryBuffer = new InMemoryBuffer();

        while (inMemoryBuffer.getEventCount() < 55) {
            inMemoryBuffer.writeEvent(generateByteArray());
        }
        assertThat(inMemoryBuffer.getSize(), greaterThan(1l));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(55));
        assertThat(inMemoryBuffer.getDuration(), greaterThanOrEqualTo(0L));

    }

    @Test
    void test_without_write_event_into_buffer() {
        inMemoryBuffer = new InMemoryBuffer();
        assertThat(inMemoryBuffer.getSize(), equalTo(0L));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(0));
        assertThat(inMemoryBuffer.getDuration(), lessThanOrEqualTo(0L));

    }

    @Test
    void test_uploadedToS3_success() throws InterruptedException {
        inMemoryBuffer = new InMemoryBuffer();
        Assertions.assertNotNull(inMemoryBuffer);
        boolean isUploadedToS3 = inMemoryBuffer.flushToS3(s3Client, "data-prepper", "log.txt", 5);
        Assertions.assertTrue(isUploadedToS3);
    }

    private byte[] generateByteArray() {
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}