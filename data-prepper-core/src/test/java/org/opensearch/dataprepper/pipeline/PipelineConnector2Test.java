/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.buffer.TestBuffer;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.DefaultLink;
import org.opensearch.dataprepper.model.trace.DefaultSpanEvent;

import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.Queue;
import java.util.Map;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;


import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.Assertions;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@RunWith(MockitoJUnitRunner.class)
public class PipelineConnector2Test {
    private static final String RECORD_DATA = "RECORD_DATA";
    private static final Record<String> RECORD = new Record<>(RECORD_DATA);
    private static Record<Event> EVENT_RECORD;
    private static Record<JacksonSpan> SPAN_RECORD;

    private static final String SINK_PIPELINE_NAME = "SINK_PIPELINE_NAME";
    private final String testKey = UUID.randomUUID().toString();
    private final String testValue = UUID.randomUUID().toString();
    private static final String TEST_TRACE_ID = UUID.randomUUID().toString();
    private static final String TEST_SPAN_ID = UUID.randomUUID().toString();
    private static final String TEST_TRACE_STATE = UUID.randomUUID().toString();
    private static final String TEST_PARENT_SPAN_ID = UUID.randomUUID().toString();
    private static final String TEST_NAME = UUID.randomUUID().toString();
    private static final String TEST_KIND = UUID.randomUUID().toString();
    private static final String TEST_START_TIME = UUID.randomUUID().toString();
    private static final String TEST_END_TIME = UUID.randomUUID().toString();
    private static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of("key1", new Date().getTime(), "key2", UUID.randomUUID().toString());
    private static final Integer TEST_DROPPED_ATTRIBUTES_COUNT = 8;
    private static final Integer TEST_DROPPED_EVENTS_COUNT = 45;
    private static final Integer TEST_DROPPED_LINKS_COUNT = 21;
    private static final String TEST_TRACE_GROUP = UUID.randomUUID().toString();
    private static final Long TEST_DURATION_IN_NANOS = 537L;
    private static final String TEST_SERVICE_NAME = UUID.randomUUID().toString();

    @Mock
    private Buffer<Record<String>> buffer;
    private List<Record<String>> recordList;
    private PipelineConnector<Record<String>> sut;

    private TestBuffer eventBuffer;
    private List<Record<Event>> eventRecordList;
    private PipelineConnector<Record<Event>> eut;

    private BlockingBuffer<Record<JacksonSpan>> spanBuffer;
    private List<Record<JacksonSpan>> spanRecordList;
    private DefaultTraceGroupFields defaultTraceGroupFields;
    private PipelineConnector<Record<JacksonSpan>> sput;
    private DefaultLink defaultLink;
    private DefaultSpanEvent defaultSpanEvent;


    @Before
    public void setup() {
        recordList = Collections.singletonList(RECORD);
        sut = new PipelineConnector<>();

        final Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(Collections.singletonMap(testKey, testValue))
                .build();
        EVENT_RECORD = new Record<>(event);
        eventRecordList = Collections.singletonList(EVENT_RECORD);
        final Queue<Record<Event>> bufferQueue = new LinkedList<>();
        eventBuffer = new TestBuffer(bufferQueue, 1);
        eut = new PipelineConnector<>();

        defaultSpanEvent = DefaultSpanEvent.builder()
                .withName(UUID.randomUUID().toString())
                .withTime(UUID.randomUUID().toString())
                .build();

        defaultLink = DefaultLink.builder()
                .withTraceId(UUID.randomUUID().toString())
                .withSpanId(UUID.randomUUID().toString())
                .withTraceState(UUID.randomUUID().toString())
                .build();

        defaultTraceGroupFields = DefaultTraceGroupFields.builder()
                .withDurationInNanos(123L)
                .withStatusCode(201)
                .withEndTime("the End")
                .build();
        final JacksonSpan span = JacksonSpan.builder()
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE)
                .withParentSpanId(TEST_PARENT_SPAN_ID)
                .withName(TEST_NAME)
                .withServiceName(TEST_SERVICE_NAME)
                .withKind(TEST_KIND)
                .withStartTime(TEST_START_TIME)
                .withEndTime(TEST_END_TIME)
                .withAttributes(TEST_ATTRIBUTES)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTES_COUNT)
                .withEvents(Arrays.asList(defaultSpanEvent))
                .withDroppedEventsCount(TEST_DROPPED_EVENTS_COUNT)
                .withLinks(Arrays.asList(defaultLink))
                .withDroppedLinksCount(TEST_DROPPED_LINKS_COUNT)
                .withTraceGroup(TEST_TRACE_GROUP)
                .withDurationInNanos(TEST_DURATION_IN_NANOS)
                .withTraceGroupFields(defaultTraceGroupFields)
                .build();

        SPAN_RECORD = new Record<>(span);
        spanRecordList = Collections.singletonList(SPAN_RECORD);
        spanBuffer = new BlockingBuffer<>("Pipeline1");
        sput = new PipelineConnector<>();

    }

    @Test(expected = RuntimeException.class)
    public void testOutputWithoutBufferInitialized() {
        sut.output(recordList);
    }

    @Test(expected = RuntimeException.class)
    public void testOutputAfterBufferStopRequested() {
        sut.start(buffer);
        assertTrue(sut.isReady());
        sut.stop();

        sut.output(recordList);
    }

    @Test
    public void testOutputBufferTimesOutThenSucceeds() throws Exception {
        doThrow(new TimeoutException()).doNothing().when(buffer).write(any(), anyInt());

        sut.start(buffer);
        assertTrue(sut.isReady());

        sut.output(recordList);

        verify(buffer, times(2)).write(eq(RECORD), anyInt());
    }

    @Test
    public void testOutputSuccess() throws Exception {
        sut.start(buffer);
        assertTrue(sut.isReady());

        sut.output(recordList);

        verify(buffer).write(eq(RECORD), anyInt());
    }

    @Test
    public void testEventBufferOutputSuccess() throws Exception {
        eut.start(eventBuffer);
        assertTrue(eut.isReady());

        eut.output(eventRecordList);

        Map.Entry<Collection<Record<Event>>, CheckpointState> ent = eventBuffer.read(1);
        ArrayList<Record<Event>> records = new ArrayList<>(ent.getKey());
        // Make sure the records are same
        assertThat(eventRecordList.get(0), sameInstance(records.get(0)));
        // Make sure the events are same
        Event event1 = eventRecordList.get(0).getData();
        Event event2 = records.get(0).getData();
        assertThat(event1, sameInstance(event2));
        event1.toMap().forEach((k, v) -> Assertions.assertEquals(event2.get(k, String.class), v));
        event1.toMap().forEach((k, v) -> Assertions.assertEquals(k, testKey));
        event1.toMap().forEach((k, v) -> Assertions.assertEquals(v, testValue));

    }

    @Test
    public void testSpanBufferOutputSuccess() throws Exception {
        sput.start(spanBuffer);
        assertTrue(sput.isReady());

        sput.output(spanRecordList);

        Map.Entry<Collection<Record<JacksonSpan>>, CheckpointState> ent = spanBuffer.doRead(10000);
        ArrayList<Record<JacksonSpan>> records = new ArrayList<>(ent.getKey());
        // Make sure the records are same
        assertThat(spanRecordList.get(0), sameInstance(records.get(0)));
        // Make sure the spans are same
        JacksonSpan span1 = spanRecordList.get(0).getData();
        JacksonSpan span2 = records.get(0).getData();
        assertThat(span1, sameInstance(span2));
        span1.toMap().forEach((k, v) -> Assertions.assertEquals(span2.toMap().get(k), v));
    }

    @Test
    public void testSetSinkPipelineName() {
        sut.setSinkPipelineName(SINK_PIPELINE_NAME);

        try {
            sut.output(recordList);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SINK_PIPELINE_NAME));
        }
    }
}
