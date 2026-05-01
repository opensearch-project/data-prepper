/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.common.utils.RetryUtil;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.util.MlCommonRequester;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BedrockProxyBatchPredictorTest {

    @Mock
    private MlCommonRequester mlCommonRequester;
    @Mock
    private MLProcessorConfig mlProcessorConfig;
    @Mock
    private MLBatchJobCreator jobNameSource;

    private BedrockProxyBatchPredictor predictor;

    @BeforeEach
    void setUp() {
        when(mlProcessorConfig.getOutputPath()).thenReturn("s3://output-bucket/results");
        when(jobNameSource.generateJobName()).thenReturn("batch-job-test");
        predictor = new BedrockProxyBatchPredictor(mlCommonRequester, mlProcessorConfig, jobNameSource);
    }

    @Test
    void predict_whenS3UriIsValid_delegatesToMlCommonsRequester() throws Exception {
        try (MockedStatic<RetryUtil> retryUtil = mockStatic(RetryUtil.class)) {
            retryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenReturn(new RetryUtil.RetryResult(true, null, 1));

            final RetryUtil.RetryResult result = predictor.predict("s3://bucket/input.jsonl");

            assertThat(result.isSuccess(), is(true));
        }
    }

    @Test
    void predict_payloadContainsS3Uri() throws Exception {
        try (MockedStatic<RetryUtil> retryUtil = mockStatic(RetryUtil.class)) {
            retryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenAnswer(invocation -> {
                        final Runnable task = invocation.getArgument(0);
                        task.run();
                        return new RetryUtil.RetryResult(true, null, 1);
                    });

            predictor.predict("s3://input-bucket/data.jsonl");

            verify(mlCommonRequester).sendRequestToMLCommons(
                    org.mockito.ArgumentMatchers.contains("s3://input-bucket/data.jsonl"));
        }
    }

    @Test
    void predict_payloadContainsOutputPath() throws Exception {
        try (MockedStatic<RetryUtil> retryUtil = mockStatic(RetryUtil.class)) {
            retryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenAnswer(invocation -> {
                        final Runnable task = invocation.getArgument(0);
                        task.run();
                        return new RetryUtil.RetryResult(true, null, 1);
                    });

            predictor.predict("s3://bucket/input.jsonl");

            verify(mlCommonRequester).sendRequestToMLCommons(
                    org.mockito.ArgumentMatchers.contains("s3://output-bucket/results"));
        }
    }

    @Test
    void predict_payloadContainsJobName() throws Exception {
        try (MockedStatic<RetryUtil> retryUtil = mockStatic(RetryUtil.class)) {
            retryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenAnswer(invocation -> {
                        final Runnable task = invocation.getArgument(0);
                        task.run();
                        return new RetryUtil.RetryResult(true, null, 1);
                    });

            predictor.predict("s3://bucket/input.jsonl");

            verify(mlCommonRequester).sendRequestToMLCommons(
                    org.mockito.ArgumentMatchers.contains("batch-job-test"));
        }
    }

    @Test
    void predict_whenS3UriIsNull_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> predictor.predict(null));
        verify(mlCommonRequester, never()).sendRequestToMLCommons(any());
    }

    @Test
    void predict_whenS3UriIsEmpty_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> predictor.predict(""));
        verify(mlCommonRequester, never()).sendRequestToMLCommons(any());
    }
}
