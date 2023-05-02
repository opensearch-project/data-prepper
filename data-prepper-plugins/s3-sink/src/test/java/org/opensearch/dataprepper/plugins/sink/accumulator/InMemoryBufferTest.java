package org.opensearch.dataprepper.plugins.sink.accumulator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.S3SinkService;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.BucketOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import java.io.IOException;
import java.time.Duration;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InMemoryBufferTest {

    private S3SinkConfig s3SinkConfig;
    private InMemoryBuffer inMemoryBuffer;
    private AwsAuthenticationOptions awsAuthenticationOptions;
    private S3Client s3Client;
    private AwsCredentialsProvider awsCredentialsProvider;
    private BucketOptions bucketOptions;
    private ObjectKeyOptions objectKeyOptions;
    private ThresholdOptions thresholdOptions;

    @BeforeEach
    void setUp() {
        s3SinkConfig = mock(S3SinkConfig.class);
        s3Client = mock(S3Client.class);
        bucketOptions = mock(BucketOptions.class);
        objectKeyOptions = mock(ObjectKeyOptions.class);
        awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        thresholdOptions = mock(ThresholdOptions.class);

        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");
        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(10);
        when(s3SinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse("1kb"));
        when(s3SinkConfig.getThresholdOptions().getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(5));
        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.INMEMORY);
        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(s3SinkConfig.getBucketOptions().getBucketName()).thenReturn("dataprepper");
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions().getPathPrefix()).thenReturn("logdata/");
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of("us-east-1"));
        when(awsAuthenticationOptions.authenticateAwsConfiguration()).thenReturn(awsCredentialsProvider);
        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
    }

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
    void test_uploadedToS3_success() {
        inMemoryBuffer = new InMemoryBuffer();
        Assertions.assertNotNull(inMemoryBuffer);
        boolean isUploadedToS3 = inMemoryBuffer.flushToS3(s3Client, "data-prepper", "log.txt");
        Assertions.assertTrue(isUploadedToS3);
    }

    @Test
    void test_uploadedToS3_failure() {
        inMemoryBuffer = new InMemoryBuffer();
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, null, null);
        S3Client s3Client = s3SinkService.createS3Client();
        Assertions.assertNotNull(inMemoryBuffer);
        boolean isUploadedToS3 = inMemoryBuffer.flushToS3(s3Client, null, "log.txt");
        Assertions.assertFalse(isUploadedToS3);
    }

    private byte[] generateByteArray() {
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}