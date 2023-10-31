/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.parser.DataFlowComponent;
import org.opensearch.dataprepper.pipeline.PipelineConnector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.Assert.assertFalse;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;

@ExtendWith(MockitoExtension.class)
public class RouterCopyRecordStrategyTests {

    @Mock
    private DataFlowComponent<PipelineConnector> pipelineDataFlowComponent;
    private Collection<Record> recordsIn;
    private Collection<Record> mockRecordsIn;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private AcknowledgementSet acknowledgementSet1;
    @Mock
    private EventFactory eventFactory;
    @Mock
    private EventBuilder eventBuilder;

    private JacksonEvent event;

    private Map<DefaultEventHandle, Integer> handleRefCount;

    private static class TestComponent {
    }

    @BeforeEach
    void setUp() {
        handleRefCount = new HashMap<>();
        eventFactory = mock(EventFactory.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        acknowledgementSet1 = mock(AcknowledgementSet.class);
        try {
            lenient().doAnswer((i) -> {
                DefaultEventHandle handle = (DefaultEventHandle) i.getArgument(0);
                int v = handleRefCount.getOrDefault(handle, 0);
                handleRefCount.put(handle, v+1);
                return null;
            }).when(acknowledgementSetManager).acquireEventReference(any(DefaultEventHandle.class));
        } catch (Exception e){}
        mockRecordsIn = IntStream.range(0, 10)
                .mapToObj(i -> mock(Record.class))
                .collect(Collectors.toList());
        final Map<String, Object> eventMap = new HashMap<>();
        eventMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        event = JacksonEvent.builder().withData(eventMap)
                                      .withEventType("event")
                                      .build();
        recordsIn = IntStream.range(0, 10)
                .mapToObj(i -> new Record<Event>(JacksonEvent.fromEvent(event)))
                .collect(Collectors.toList());
    }

    private void attachEventHandlesToRecordsIn(List<DefaultEventHandle> eventHandles) {
        Iterator iter = recordsIn.iterator();
        while (iter.hasNext()) {
            Record r = (Record) iter.next();
            DefaultEventHandle handle = (DefaultEventHandle)((JacksonEvent)r.getData()).getEventHandle();
            handle.setAcknowledgementSet(acknowledgementSet1);
            eventHandles.add(handle);
        }
    }

    private <C> RouterCopyRecordStrategy createObjectUnderTest(Collection<DataFlowComponent<C>> dataFlowComponents) {
        return new RouterCopyRecordStrategy(eventFactory, acknowledgementSetManager, dataFlowComponents);
    }

    @Test
    void test_invalid_argument() {
        assertThrows(NullPointerException.class, () -> createObjectUnderTest(null));
    }

    @Test
    void test_with_one_data_flow_component() {
        DataFlowComponent<TestComponent> dataFlowComponent = mock(DataFlowComponent.class);
        Collection<DataFlowComponent<TestComponent>> dataFlowComponents
            = Collections.singletonList(dataFlowComponent);

        final RouterCopyRecordStrategy getRecordStrategy = createObjectUnderTest(dataFlowComponents);
        Record firstRecord = mockRecordsIn.iterator().next();
        Record recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        Collection<Record> recordsOut = getRecordStrategy.getAllRecords(mockRecordsIn);
        assertThat(recordsOut.size(), equalTo(mockRecordsIn.size()));
        assertThat(mockRecordsIn.stream().collect(Collectors.toSet()), equalTo(recordsOut.stream().collect(Collectors.toSet())));
    }

    @Test
    void test_with_one_data_flow_pipeline_connector() {
        DataFlowComponent<PipelineConnector> dataFlowComponent = mock(DataFlowComponent.class);

        Collection<DataFlowComponent<PipelineConnector>> dataFlowComponents
            = Collections.singletonList(dataFlowComponent);
        final RouterCopyRecordStrategy getRecordStrategy = createObjectUnderTest(dataFlowComponents);
        Record firstRecord = mockRecordsIn.iterator().next();
        Record recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        Collection<Record> recordsOut = getRecordStrategy.getAllRecords(mockRecordsIn);
        assertThat(recordsOut.size(), equalTo(mockRecordsIn.size()));
        assertThat(mockRecordsIn.stream().collect(Collectors.toSet()), equalTo(recordsOut.stream().collect(Collectors.toSet())));
        /* Repeat the test to see if the conditions holds true */
        recordsOut = getRecordStrategy.getAllRecords(mockRecordsIn);
        assertThat(recordsOut.size(), equalTo(mockRecordsIn.size()));
        assertThat(mockRecordsIn.stream().collect(Collectors.toSet()), equalTo(recordsOut.stream().collect(Collectors.toSet())));
    }

    @Test
    void test_with_multiple_data_flow_components_and_pipeline_connectors() {
        Collection<DataFlowComponent<PipelineConnector>> dataFlowComponents = new ArrayList<>();
        when(pipelineDataFlowComponent.getComponent()).thenReturn(new PipelineConnector());
        for (int i = 0; i < 3; i++) {
            dataFlowComponents.add(pipelineDataFlowComponent);
        }

        final RouterCopyRecordStrategy getRecordStrategy = createObjectUnderTest(dataFlowComponents);
        Record firstRecord = recordsIn.iterator().next();
        Record recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, not(sameInstance(firstRecord)));
        Collection<Record> recordsOut = getRecordStrategy.getAllRecords(recordsIn);
        assertThat(recordsOut.size(), equalTo(recordsIn.size()));
        /* Repeat the test to make sure the records are copied second time */
        recordsOut = getRecordStrategy.getAllRecords(recordsIn);
        assertThat(recordsOut.size(), equalTo(recordsIn.size()));

        Set<Record> recordsOutSet =  recordsOut.stream().collect(Collectors.toSet());
        recordsIn.forEach(recordIn -> assertFalse(recordsOutSet.contains(recordIn)));
    }

    @Test
    void test_one_record_with_acknowledgements() {
        DataFlowComponent<TestComponent> dataFlowComponent = mock(DataFlowComponent.class);
        Collection<DataFlowComponent<TestComponent>> dataFlowComponents
            = Collections.singletonList(dataFlowComponent);

        final RouterCopyRecordStrategy getRecordStrategy = createObjectUnderTest(dataFlowComponents);
        List<DefaultEventHandle> eventHandles = new ArrayList<>();
        attachEventHandlesToRecordsIn(eventHandles);
        Record firstRecord = recordsIn.iterator().next();
        DefaultEventHandle firstHandle = (DefaultEventHandle)((Event)firstRecord.getData()).getEventHandle();
        Record recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        assertTrue(getRecordStrategy.getReferencedRecords().contains(firstRecord));
        recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        assertThat(handleRefCount.get(firstHandle), equalTo(1));
        recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        assertThat(handleRefCount.get(firstHandle), equalTo(2));
    }

    @Test
    void test_multiple_records_with_acknowledgements() {
        DataFlowComponent<TestComponent> dataFlowComponent = mock(DataFlowComponent.class);
        Collection<DataFlowComponent<TestComponent>> dataFlowComponents
            = Collections.singletonList(dataFlowComponent);

        final RouterCopyRecordStrategy getRecordStrategy = createObjectUnderTest(dataFlowComponents);
        List<DefaultEventHandle> eventHandles = new ArrayList<>();
        attachEventHandlesToRecordsIn(eventHandles);
        Collection<Record> recordsOut = getRecordStrategy.getAllRecords(recordsIn);
        assertThat(recordsOut.size(), equalTo(recordsIn.size()));
        assertThat(recordsIn.stream().collect(Collectors.toSet()), equalTo(recordsOut.stream().collect(Collectors.toSet())));
        recordsIn.stream().forEach((record) -> {
            assertTrue(getRecordStrategy.getReferencedRecords().contains(record));
        });
        recordsOut = getRecordStrategy.getAllRecords(recordsIn);
        assertThat(recordsOut.size(), equalTo(recordsIn.size()));
        assertThat(recordsIn.stream().collect(Collectors.toSet()), equalTo(recordsOut.stream().collect(Collectors.toSet())));
        eventHandles.stream().forEach((handle) -> {
            assertThat(handleRefCount.get(handle), equalTo(1));
        });
        recordsOut = getRecordStrategy.getAllRecords(recordsIn);
        eventHandles.stream().forEach((handle) -> {
            assertThat(handleRefCount.get(handle), equalTo(2));
        });
    }

    @Test
    void test_one_record_with_acknowledgements_and_multi_components() {
        Collection<DataFlowComponent<PipelineConnector>> dataFlowComponents = new ArrayList<>();
        when(pipelineDataFlowComponent.getComponent()).thenReturn(new PipelineConnector());
        for (int i = 0; i < 3; i++) {
            dataFlowComponents.add(pipelineDataFlowComponent);
        }

        final RouterCopyRecordStrategy getRecordStrategy = createObjectUnderTest(dataFlowComponents);
        List<DefaultEventHandle> eventHandles = new ArrayList<>();
        attachEventHandlesToRecordsIn(eventHandles);
        try {
            doAnswer((i) -> {
                JacksonEvent e1 = (JacksonEvent) i.getArgument(0);
                ((DefaultEventHandle)e1.getEventHandle()).setAcknowledgementSet(acknowledgementSet1);
                return null;
            }).when(acknowledgementSet1).add(any(JacksonEvent.class));
        } catch (Exception e){}

        eventBuilder = mock(EventBuilder.class);
        when(eventBuilder.withEventMetadata(any(EventMetadata.class))).thenReturn(eventBuilder);
        when(eventBuilder.withData(any(Map.class))).thenReturn(eventBuilder);
        when(eventBuilder.build()).thenReturn(JacksonEvent.fromEvent(event));
        when(eventFactory.eventBuilder(EventBuilder.class)).thenReturn(eventBuilder);
        Record firstRecord = recordsIn.iterator().next();
        DefaultEventHandle firstHandle = (DefaultEventHandle)((Event)firstRecord.getData()).getEventHandle();
        Record recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        assertTrue(getRecordStrategy.getReferencedRecords().contains(firstRecord));
        recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, not(sameInstance(firstRecord)));
        assertFalse(handleRefCount.containsKey(firstHandle));
        DefaultEventHandle newHandle = (DefaultEventHandle)((JacksonEvent)recordOut.getData()).getEventHandle();
        assertTrue(getRecordStrategy.getReferencedRecords().contains(recordOut));
        assertThat(newHandle, not(equalTo(null)));
        assertFalse(handleRefCount.containsKey(newHandle));
    }

    @Test
    void test_multiple_records_with_acknowledgements_and_multi_components() {
        Collection<DataFlowComponent<PipelineConnector>> dataFlowComponents = new ArrayList<>();
        when(pipelineDataFlowComponent.getComponent()).thenReturn(new PipelineConnector());
        for (int i = 0; i < 3; i++) {
            dataFlowComponents.add(pipelineDataFlowComponent);
        }

        final RouterCopyRecordStrategy getRecordStrategy = createObjectUnderTest(dataFlowComponents);
        List<DefaultEventHandle> eventHandles = new ArrayList<>();
        attachEventHandlesToRecordsIn(eventHandles);
        try {
            doAnswer((i) -> {
                JacksonEvent e1 = (JacksonEvent) i.getArgument(0);
                ((DefaultEventHandle)e1.getEventHandle()).setAcknowledgementSet(acknowledgementSet1);
                return null;
            }).when(acknowledgementSet1).add(any(JacksonEvent.class));
        } catch (Exception e){}

        eventBuilder = mock(EventBuilder.class);
        when(eventBuilder.withEventMetadata(any(EventMetadata.class))).thenReturn(eventBuilder);
        when(eventBuilder.withData(any(Map.class))).thenReturn(eventBuilder);
        when(eventBuilder.build()).thenReturn(JacksonEvent.fromEvent(event));
        when(eventFactory.eventBuilder(EventBuilder.class)).thenReturn(eventBuilder);
        Collection<Record> recordsOut = getRecordStrategy.getAllRecords(recordsIn);
        assertThat(recordsOut.size(), equalTo(recordsIn.size()));

        recordsOut = getRecordStrategy.getAllRecords(recordsIn);
        assertThat(recordsOut.size(), equalTo(recordsIn.size()));

        Set<Record> recordsOutSet =  recordsOut.stream().collect(Collectors.toSet());
        recordsIn.forEach(recordIn -> assertFalse(recordsOutSet.contains(recordIn)));

        recordsIn.stream().forEach((record) -> {
            assertTrue(getRecordStrategy.getReferencedRecords().contains(record));
            JacksonEvent e = (JacksonEvent)record.getData();
            assertFalse(handleRefCount.containsKey(e.getEventHandle()));
        });
        recordsOut.stream().forEach((record) -> {
            assertTrue(getRecordStrategy.getReferencedRecords().contains(record));
            JacksonEvent e = (JacksonEvent)record.getData();
            assertFalse(handleRefCount.containsKey(e.getEventHandle()));
        });
    }
}
