/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.slf4j.Logger;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

public class LambdaCommonHandlerTest {

  @Mock
  private Logger mockLogger;

  @Mock
  private LambdaAsyncClient mockLambdaAsyncClient;


  @Mock
  private Buffer mockBuffer;

  @Mock
  private InvokeResponse mockInvokeResponse;

  private LambdaCommonHandler lambdaCommonHandler;

  private String functionName = "test-function";

  private String invocationType = InvocationType.REQUEST_RESPONSE.getAwsLambdaValue();

  @Mock
  private LambdaCommonConfig lambdaCommonConfig;
  @Mock
  private JsonOutputCodecConfig jsonOutputCodecConfig;
  @Mock
  private InvocationType invType;

  @Mock
  private BatchOptions batchOptions;

  private ThresholdOptions thresholdOptions;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(jsonOutputCodecConfig.getKeyName()).thenReturn("test");
    when(invType.getAwsLambdaValue()).thenReturn(
        InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
    when(lambdaCommonConfig.getBatchOptions()).thenReturn(batchOptions);
    thresholdOptions = new ThresholdOptions();
    when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);
    when(lambdaCommonConfig.getInvocationType()).thenReturn(invType);
    when(lambdaCommonConfig.getFunctionName()).thenReturn(functionName);
  }

  @Test
  public void testCreateBuffer_success() throws IOException {

    //TODO: need a better test here
  }

  @Test
  public void testCreateBuffer_throwsException() throws IOException {
    // Arrange
    //TODO: need a better test here
  }

  @Test
  public void testWaitForFutures_allComplete() {
    // Arrange
    //TODO: need a better test here
  }

  @Test
  public void testWaitForFutures_withException() {
    // Arrange
    //TODO: need a better test here
  }

  private List<EventHandle> mockEventHandleList(int size) {
    List<EventHandle> eventHandleList = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      EventHandle eventHandle = mock(EventHandle.class);
      eventHandleList.add(eventHandle);
    }
    return eventHandleList;
  }

}
