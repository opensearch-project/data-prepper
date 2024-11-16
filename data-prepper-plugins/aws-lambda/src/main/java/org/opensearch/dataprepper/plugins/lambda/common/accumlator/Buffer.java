/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

/**
 * A buffer can hold data before flushing it.
 */
public interface Buffer {

  long getSize();

  int getEventCount();

  void setEventCount(int eventCount);

  Duration getDuration();

  InvokeRequest getRequestPayload(String functionName, String invocationType);

  OutputStream getOutputStream();

  SdkBytes getPayload();

  void addRecord(Record<Event> record);

  List<Record<Event>> getRecords();

  Duration getFlushLambdaLatencyMetric();

  Long getPayloadRequestSize();

  Duration stopLatencyWatch();

  void reset();

}
