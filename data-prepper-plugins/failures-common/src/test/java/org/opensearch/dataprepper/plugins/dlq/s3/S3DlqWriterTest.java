/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.dlq.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.failures.DlqObject;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.dlq.s3.S3DlqWriter.S3_DLQ_RECORDS_FAILED;
import static org.opensearch.dataprepper.plugins.dlq.s3.S3DlqWriter.S3_DLQ_RECORDS_SUCCESS;
import static org.opensearch.dataprepper.plugins.dlq.s3.S3DlqWriter.S3_DLQ_REQUEST_FAILED;
import static org.opensearch.dataprepper.plugins.dlq.s3.S3DlqWriter.S3_DLQ_REQUEST_LATENCY;
import static org.opensearch.dataprepper.plugins.dlq.s3.S3DlqWriter.S3_DLQ_REQUEST_SIZE_BYTES;
import static org.opensearch.dataprepper.plugins.dlq.s3.S3DlqWriter.S3_DLQ_REQUEST_SUCCESS;

@ExtendWith(MockitoExtension.class)
public class S3DlqWriterTest {

    @Mock
    private S3DlqWriterConfig config;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private S3Client s3Client;

    @Mock
    private SdkHttpResponse mockHttpResponse;

    @Mock
    private Counter dlqS3RecordsSuccessCounter;
    @Mock
    private  Counter dlqS3RecordsFailedCounter;

    @Mock
    private  Counter dlqS3RequestSuccessCounter;

    @Mock
    private  Counter dlqS3RequestFailedCounter;
    @Mock
    private  Timer dlqS3RequestTimer;
    @Mock
    private  DistributionSummary dlqS3RequestSizeBytesSummary;

    private PutObjectResponse putObjectResponse;

    private ObjectMapper objectMapper;
    private S3DlqWriter s3DlqWriter;

    private String pipelineName;
    private String pluginName;
    private String pluginId;
    private String bucket;
    private String keyPathPrefix;
    private String expectedKeyPrefix;
    private List<DlqObject> dlqObjects;

    @BeforeEach
    public void setup() {
        pipelineName = UUID.randomUUID().toString();
        pluginName = UUID.randomUUID().toString();
        pluginId = UUID.randomUUID().toString();
        bucket = UUID.randomUUID().toString();
        keyPathPrefix = UUID.randomUUID().toString();

        expectedKeyPrefix = String.format("%sdlq-v%s-%s-%s", keyPathPrefix, DataPrepperVersion.getCurrentVersion(), pipelineName, pluginId);

        putObjectResponse = (PutObjectResponse) PutObjectResponse.builder()
            .sdkHttpResponse(mockHttpResponse)
            .build();

        final int numberOfObjects = new Random().nextInt(18) + 2;
        dlqObjects = generateDlqData(numberOfObjects);

        objectMapper = new ObjectMapper();

        when(pluginMetrics.counter(S3_DLQ_RECORDS_FAILED)).thenReturn(dlqS3RecordsFailedCounter);
        when(pluginMetrics.counter(S3_DLQ_REQUEST_FAILED)).thenReturn(dlqS3RequestFailedCounter);
        when(pluginMetrics.counter(S3_DLQ_RECORDS_SUCCESS)).thenReturn(dlqS3RecordsSuccessCounter);
        when(pluginMetrics.counter(S3_DLQ_REQUEST_SUCCESS)).thenReturn(dlqS3RequestSuccessCounter);
        when(pluginMetrics.timer(S3_DLQ_REQUEST_LATENCY)).thenReturn(dlqS3RequestTimer);
        when(pluginMetrics.summary(S3_DLQ_REQUEST_SIZE_BYTES)).thenReturn(dlqS3RequestSizeBytesSummary);
    }

    @AfterEach
    public void tearDown() {
        verifyNoMoreInteractions(s3Client);
    }

    @ParameterizedTest
    @MethodSource("validKeyPathPrefixes")
    public void testWrite(final String keyPathPrefix, final String expectedKeyPrefix) throws Exception {
        when(config.getKeyPathPrefix()).thenReturn(keyPathPrefix);
        when(config.getS3Client()).thenReturn(s3Client);
        when(config.getBucket()).thenReturn(bucket);
        when(config.getBucketOwner()).thenReturn(UUID.randomUUID().toString());
        when(dlqS3RequestTimer.recordCallable(any(Callable.class))).thenAnswer(a -> {
            try {
                return a.getArgument(0, Callable.class).call();
            } catch (final Exception ex) {
                throw ex;
            }
        });
        s3DlqWriter = new S3DlqWriter(config, objectMapper, pluginMetrics);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(putObjectResponse);
        when(mockHttpResponse.isSuccessful()).thenReturn(true);

        s3DlqWriter.write(dlqObjects, pipelineName, pluginId);

        final ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        final ArgumentCaptor<RequestBody> requestBodyArgumentCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client).putObject(putObjectRequestArgumentCaptor.capture(), requestBodyArgumentCaptor.capture());
        final PutObjectRequest putObjectRequest = putObjectRequestArgumentCaptor.getValue();

        assertThat(putObjectRequest.bucket(), is(equalTo(bucket)));
        assertThat(putObjectRequest.key(), startsWith(String.format("%s-%s-%s", expectedKeyPrefix, pipelineName, pluginId)));
        assertThat(putObjectRequest.key(), endsWith(".json"));
        assertThat(putObjectRequest.expectedBucketOwner(), equalTo(config.getBucketOwner()));
        verify(dlqS3RequestSuccessCounter).increment();
        verify(dlqS3RecordsSuccessCounter).increment(dlqObjects.size());
    }

    @Test
    void write_with_empty_list_does_not_write_to_S3() throws Exception {
        when(config.getKeyPathPrefix()).thenReturn(keyPathPrefix);
        when(config.getS3Client()).thenReturn(s3Client);
        when(config.getBucket()).thenReturn(bucket);
        s3DlqWriter = new S3DlqWriter(config, objectMapper, pluginMetrics);

        dlqObjects = Collections.emptyList();

        s3DlqWriter.write(dlqObjects, pipelineName, pluginId);

        verifyNoInteractions(s3Client);
        verifyNoInteractions(dlqS3RequestSuccessCounter);
        verifyNoInteractions(dlqS3RecordsSuccessCounter);
        verifyNoInteractions(dlqS3RequestFailedCounter);
        verifyNoInteractions(dlqS3RecordsFailedCounter);
    }

    private static Stream<Arguments> validKeyPathPrefixes() {
        final String randomKeyPathPrefix = UUID.randomUUID().toString();

        return Stream.of(
            Arguments.of(randomKeyPathPrefix, String.format("%s/dlq-v%s", randomKeyPathPrefix, DataPrepperVersion.getCurrentVersion().getMajorVersion())),
            Arguments.of(randomKeyPathPrefix + "/", String.format("%s/dlq-v%s", randomKeyPathPrefix, DataPrepperVersion.getCurrentVersion().getMajorVersion())),
            Arguments.of(null, String.format("dlq-v%s", DataPrepperVersion.getCurrentVersion().getMajorVersion())),
            Arguments.of("", String.format("dlq-v%s", DataPrepperVersion.getCurrentVersion().getMajorVersion()))
        );
    }

    @Nested
    class S3ClientFailures {

        @BeforeEach
        public void setup() throws Exception {
            when(config.getS3Client()).thenReturn(s3Client);
            when(config.getBucket()).thenReturn(bucket);
            s3DlqWriter = new S3DlqWriter(config, objectMapper, pluginMetrics);
            when(dlqS3RequestTimer.recordCallable(any(Callable.class))).thenAnswer(a -> {
                try {
                    return a.getArgument(0, Callable.class).call();
                } catch (final Exception ex) {
                    throw ex;
                }
            });
        }

        @AfterEach
        public void tearDown() {
            verify(dlqS3RequestFailedCounter).increment();
            verify(dlqS3RecordsFailedCounter).increment(dlqObjects.size());
        }

        @Test
        public void testPutObjectRequestIsUnsuccessfulS3Request() {
            when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(putObjectResponse);
            when(mockHttpResponse.isSuccessful()).thenReturn(false);

            assertThrows(IOException.class, () -> s3DlqWriter.write(dlqObjects, pipelineName, pluginId));
        }

        @Test
        public void testPutObjectRequestS3ClientThrowsException() {
            when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(SdkClientException.class);

            assertThrows(IOException.class, () -> s3DlqWriter.write(dlqObjects, pipelineName, pluginId));
        }
    }

    @Nested
    class ObjectMapperFailures {

        @Mock
        private ObjectMapper objectMapper;

        private S3DlqWriter s3DlqWriter;

        @AfterEach
        public void tearDown() {
            verify(dlqS3RequestFailedCounter).increment();
            verify(dlqS3RecordsFailedCounter).increment(dlqObjects.size());
        }

        @BeforeEach
        public void setup() {
            when(config.getS3Client()).thenReturn(s3Client);
            when(config.getBucket()).thenReturn(bucket);
            s3DlqWriter = new S3DlqWriter(config, objectMapper, pluginMetrics);
        }

        @Test
        public void testDeserializeThrowsException() throws JsonProcessingException {
            when(objectMapper.writeValueAsString(any())).thenThrow(JsonProcessingException.class);

            assertThrows(IOException.class, () -> s3DlqWriter.write(dlqObjects, pipelineName, pluginId));
        }
    }

    @Nested
    class Constructor {

        @Test
        public void testMissingBucketThrowsException() {
            final S3DlqWriterConfig s3DlqWriterConfig = new S3DlqWriterConfig();
            assertThrows(NullPointerException.class, () -> new S3DlqWriter(s3DlqWriterConfig, objectMapper, pluginMetrics));
        }
    }

    @Test
    public void testClose() throws IOException {
        when(config.getS3Client()).thenReturn(s3Client);
        when(config.getBucket()).thenReturn(bucket);
        s3DlqWriter = new S3DlqWriter(config, objectMapper, pluginMetrics);
        doNothing().when(s3Client).close();

        s3DlqWriter.close();
        verify(s3Client).close();
    }

    private List<DlqObject> generateDlqData(final int numberOfObjects) {
        ImmutableList.Builder<DlqObject> dlqObjects = ImmutableList.builder();
        for (int i = 0; i < numberOfObjects; i++) {
            dlqObjects.add(DlqObject.builder()
                    .withFailedData(UUID.randomUUID())
                    .withPipelineName(pipelineName)
                    .withPluginId(pluginId)
                    .withPluginName(pluginName)
                    .build());
        }
        return dlqObjects.build();
    }
}
