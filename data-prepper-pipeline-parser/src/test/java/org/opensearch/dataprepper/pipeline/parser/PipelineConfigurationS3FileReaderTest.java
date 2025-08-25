package org.opensearch.dataprepper.pipeline.parser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelineConfigurationS3FileReaderTest {
    private static final Region TEST_REGION = Region.US_EAST_1;

    @Mock
    private S3Client s3Client;

    private PipelineConfigurationS3FileReader reader;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testReadSingleValidS3Path() throws IOException {
        // Arrange
        String configContent = "pipeline:\n  name: test";
        String s3Path = "s3://mybucket/config.yaml";
        mockS3Response(s3Path, configContent);

        reader = createReaderWithMockS3Client(List.of(s3Path));

        // Act
        List<InputStream> streams = reader.getInputStreamsForConfigurationFiles();

        // Assert
        assertThat(streams, hasSize(1));
        String result = new String(streams.get(0).readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(configContent, result);
    }

    @Test
    void testReadMultipleValidS3Paths() throws IOException {
        // Arrange
        String configContent1 = "pipeline1:\n  name: test1";
        String configContent2 = "pipeline2:\n  name: test2";
        String s3Path1 = "s3://mybucket/config1.yaml";
        String s3Path2 = "s3://mybucket/config2.yaml";

        mockS3Response(s3Path1, configContent1);
        mockS3Response(s3Path2, configContent2);

        reader = createReaderWithMockS3Client(Arrays.asList(s3Path1, s3Path2));

        // Act
        List<InputStream> streams = reader.getInputStreamsForConfigurationFiles();

        // Assert
        assertThat(streams, hasSize(2));
        assertEquals(configContent1, new String(streams.get(0).readAllBytes(), StandardCharsets.UTF_8));
        assertEquals(configContent2, new String(streams.get(1).readAllBytes(), StandardCharsets.UTF_8));
    }

    @Test
    void testInvalidS3Path() {
        // Arrange
        String invalidPath = "invalid://mybucket/config.yaml";
        reader = createReaderWithMockS3Client(List.of(invalidPath));

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () ->
                reader.getInputStreamsForConfigurationFiles()
        );
    }

    @Test
    void testS3ClientError() throws IOException {
        // Arrange
        String s3Path = "s3://mybucket/config.yaml";
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(new RuntimeException("S3 Error"));

        reader = createReaderWithMockS3Client(List.of(s3Path));

        // Act & Assert
        assertThrows(RuntimeException.class, () ->
                reader.getInputStreamsForConfigurationFiles()
        );
    }

    private PipelineConfigurationS3FileReader createReaderWithMockS3Client(List<String> paths) {
        return new TestPipelineConfigurationS3FileReader(paths, TEST_REGION);
    }

    private void mockS3Response(String s3Path, String content) throws IOException {
        Matcher matcher = Pattern.compile("s3://([^/]+)/(.*)").matcher(s3Path);
        if (matcher.matches()) {
            String bucket = matcher.group(1);
            String key = matcher.group(2);

            byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
            ByteArrayInputStream byteStream = new ByteArrayInputStream(contentBytes);
            ResponseInputStream<GetObjectResponse> responseStream = new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    byteStream
            );

            GetObjectRequest expectedRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            when(s3Client.getObject(expectedRequest)).thenReturn(responseStream);
        }
    }

    class TestPipelineConfigurationS3FileReader extends PipelineConfigurationS3FileReader {
        TestPipelineConfigurationS3FileReader(List<String> paths, Region region) {
            super(paths, region);
        }

        @Override
        String readConfigurationFromS3(String s3path, S3Client client) throws IOException {
            return super.readConfigurationFromS3(s3path, s3Client);
        }
    }
}
