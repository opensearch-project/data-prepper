/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;
import org.opensearch.dataprepper.plugins.dlq.s3.S3DlqWriterConfig;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.DlqConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class RetryConfigurationTests {
    private static final String INVALID_MAX_RETRIES_CONFIG = "test-configurations/invalid-max-retries-config.yaml";
    private static final String NO_DLQ_FILE_PATH = "test-configurations/no-dlq-file-path-config.yaml";
    private static final String DLQ_FILE_PATH_10_RETRIES = "test-configurations/dlq-file-path-10-retries-config.yaml";
    private static final String WITH_DLQ_PLUGIN_10_RETRIES = "test-configurations/with-dlq-plugin-10-retries-config.yaml";
    private static final String DLQ_FILE_AND_DLQ_PLUGIN = "test-configurations/dlq-file-and-dlq-plugin-config.yaml";

    ObjectMapper objectMapper;

    @Test
    public void testDefaultConfigurationIsNotNull() {
        final RetryConfiguration retryConfiguration = new RetryConfiguration.Builder().build();
        assertNull(retryConfiguration.getDlqFile());
        assertEquals(retryConfiguration.getMaxRetries(), Integer.MAX_VALUE);
    }

    @Test
    public void testReadRetryConfigInvalidMaxRetries() throws IOException {
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generateOpenSearchSourceConfig(INVALID_MAX_RETRIES_CONFIG));
        assertThrows(IllegalArgumentException.class, () -> retryConfiguration.getMaxRetries());
    }

    @Test
    public void testReadRetryConfigNoDLQFilePath() throws IOException {
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generateOpenSearchSourceConfig(NO_DLQ_FILE_PATH));
        assertNull(retryConfiguration.getDlqFile());
        assertEquals(retryConfiguration.getMaxRetries(), Integer.MAX_VALUE);
        assertFalse(retryConfiguration.getDlq().isPresent());
    }

    @Test
    public void testReadRetryConfigWithDLQFilePath() throws IOException {
        final String fakeDlqFilePath = "foo.txt";
        final int maxRetries = 10;
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generateOpenSearchSourceConfig(DLQ_FILE_PATH_10_RETRIES));
        assertEquals(fakeDlqFilePath, retryConfiguration.getDlqFile());
        assertEquals(maxRetries, retryConfiguration.getMaxRetries());
    }

    @Test
    public void testReadRetryConfigWithDLQPlugin() throws IOException {
        final int maxRetries = 10;
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generateOpenSearchSourceConfig(WITH_DLQ_PLUGIN_10_RETRIES));
        Optional<DlqConfiguration> dlqConfiguration = retryConfiguration.getDlq();
        assertInstanceOf(S3DlqWriterConfig.class, dlqConfiguration.get().getS3DlqWriterConfig());
        assertEquals(maxRetries, retryConfiguration.getMaxRetries());
    }

    @Test
    public void testReadRetryConfigWithDLQPluginAndDLQFilePath() {
        assertThrows(RuntimeException.class, () -> OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSourceConfig(DLQ_FILE_AND_DLQ_PLUGIN)));
    }

    private OpenSearchSinkConfig generateOpenSearchSourceConfig(String yamlFile) throws IOException {
        final File configurationFile = new File(getClass().getClassLoader().getResource(yamlFile).getFile());
        objectMapper = new ObjectMapper(new YAMLFactory());
        final Map<String, Object> pipelineConfig = objectMapper.readValue(configurationFile, Map.class);
        final Map<String, Object> sinkMap = (Map<String, Object>) pipelineConfig.get("sink");
        final Map<String, Object> opensearchSinkMap = (Map<String, Object>) sinkMap.get("opensearch");
        String json = objectMapper.writeValueAsString(opensearchSinkMap);
        OpenSearchSinkConfig openSearchSinkConfig = objectMapper.readValue(json, OpenSearchSinkConfig.class);

        return openSearchSinkConfig;
    }
}
