package org.opensearch.dataprepper.plugins.sink.accumulator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;
import java.io.IOException;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ExtendWith(MockitoExtension.class)
class InMemoryBufferTest {

    public static final int MAX_EVENTS = 55;
    public static final int MAX_RETRIES = 5;
    @Mock
    private S3Client s3Client;
    private InMemoryBuffer inMemoryBuffer;

    @Test
    void test_with_write_event_into_buffer() throws IOException {
        inMemoryBuffer = new InMemoryBuffer();

        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            inMemoryBuffer.writeEvent(generateByteArray());
        }
        assertThat(inMemoryBuffer.getSize(), greaterThanOrEqualTo(54110L));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(MAX_EVENTS));
        assertThat(inMemoryBuffer.getDuration(), greaterThanOrEqualTo(0L));

    }

    @Test
    void test_with_write_event_into_buffer_and_flush_toS3() throws IOException, InterruptedException {
        inMemoryBuffer = new InMemoryBuffer();

        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            inMemoryBuffer.writeEvent(generateByteArray());
        }
        assertThat(inMemoryBuffer.getSize(), greaterThanOrEqualTo(54110L));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(MAX_EVENTS));
        assertThat(inMemoryBuffer.getDuration(), greaterThanOrEqualTo(0L));

        boolean isUploadedToS3 = inMemoryBuffer.flushToS3(s3Client, "data-prepper", "log.txt", MAX_RETRIES);
        Assertions.assertTrue(isUploadedToS3);
    }

    @Test
    void test_uploadedToS3_success() throws InterruptedException {
        inMemoryBuffer = new InMemoryBuffer();
        Assertions.assertNotNull(inMemoryBuffer);
        boolean isUploadedToS3 = inMemoryBuffer.flushToS3(s3Client, "data-prepper", "log.txt", MAX_RETRIES);
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