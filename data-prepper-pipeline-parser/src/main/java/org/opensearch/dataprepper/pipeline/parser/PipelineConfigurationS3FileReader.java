package org.opensearch.dataprepper.pipeline.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PipelineConfigurationS3FileReader implements PipelineConfigurationReader {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationS3FileReader.class);
    private static final Pattern S3_PATH_PATTERN = Pattern.compile("s3://([^/]+)/(.+)");
    private final List<String> pipelineConfigS3Paths;
    private final Region s3Region;

    public PipelineConfigurationS3FileReader(List<String> pipelineConfigS3Paths, Region s3Region) {
        this.pipelineConfigS3Paths = pipelineConfigS3Paths;
        this.s3Region = s3Region;
    }

    @Override
    public List<InputStream> getPipelineConfigurationInputStreams() {
        return getInputStreamsForConfigurationFiles();
    }

    List<InputStream> getInputStreamsForConfigurationFiles() {
        try (S3Client client = S3Client.builder().region(this.s3Region).build()) {
            return pipelineConfigS3Paths.stream().map(s3Path -> {
                try {
                    return readConfigurationFromS3(s3Path, client);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read configuration from S3", e);
                }
            }).map(config -> new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))).collect(Collectors.toList());
        }
    }

    String readConfigurationFromS3(final String s3path, final S3Client client) throws IOException {
        final Matcher matcher = S3_PATH_PATTERN.matcher(s3path);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid S3 path format. Expected: s3://bucket/key");
        }

        final String bucketName = matcher.group(1);
        final String key = matcher.group(2);

        LOG.info("Reading configuration from S3: bucket={}, key={}", bucketName, key);

        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        try (final ResponseInputStream<GetObjectResponse> s3Object = client.getObject(getObjectRequest);
             final BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }
}
