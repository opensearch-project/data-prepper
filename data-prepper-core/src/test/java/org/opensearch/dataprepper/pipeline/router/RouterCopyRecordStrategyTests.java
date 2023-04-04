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
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.Assert.assertFalse;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;

@ExtendWith(MockitoExtension.class)
public class RouterCopyRecordStrategyTests {

    @Mock
    private DataFlowComponent<PipelineConnector> pipelineDataFlowComponent;
    private Collection<Record> recordsIn;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private EventFactory eventFactory;

    private static class TestComponent {
    }

    @BeforeEach
    void setUp() {
        eventFactory = mock(EventFactory.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        recordsIn = IntStream.range(0, 10)
                .mapToObj(i -> mock(Record.class))
                .collect(Collectors.toList());
    }

    private <C> RouterGetRecordStrategy createObjectUnderTest(Collection<DataFlowComponent<C>> dataFlowComponents) {
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
        
        final RouterGetRecordStrategy getRecordStrategy = createObjectUnderTest(dataFlowComponents);
        Record firstRecord = recordsIn.iterator().next();
        Record recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        Collection<Record> recordsOut = getRecordStrategy.getAllRecords(recordsIn);
        assertThat(recordsOut.size(), equalTo(recordsIn.size()));
        assertThat(recordsIn.stream().collect(Collectors.toSet()), equalTo(recordsOut.stream().collect(Collectors.toSet())));
    }

    @Test
    void test_with_one_data_flow_pipeline_connector() {
        DataFlowComponent<PipelineConnector> dataFlowComponent = mock(DataFlowComponent.class);
        
        Collection<DataFlowComponent<PipelineConnector>> dataFlowComponents
            = Collections.singletonList(dataFlowComponent);
        final RouterGetRecordStrategy getRecordStrategy = createObjectUnderTest(dataFlowComponents);
        Record firstRecord = recordsIn.iterator().next();
        Record recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        recordOut = getRecordStrategy.getRecord(firstRecord);
        assertThat(recordOut, sameInstance(firstRecord));
        Collection<Record> recordsOut = getRecordStrategy.getAllRecords(recordsIn);
        assertThat(recordsOut.size(), equalTo(recordsIn.size()));
        assertThat(recordsIn.stream().collect(Collectors.toSet()), equalTo(recordsOut.stream().collect(Collectors.toSet())));
        /* Repeat the test to see if the conditions holds true */
        recordsOut = getRecordStrategy.getAllRecords(recordsIn);
        assertThat(recordsOut.size(), equalTo(recordsIn.size()));
        assertThat(recordsIn.stream().collect(Collectors.toSet()), equalTo(recordsOut.stream().collect(Collectors.toSet())));
    }

    @Test
    void test_with_multiple_data_flow_components_and_pipeline_connectors() {
        Collection<DataFlowComponent<PipelineConnector>> dataFlowComponents = new ArrayList<>();
        when(pipelineDataFlowComponent.getComponent()).thenReturn(new PipelineConnector());
        for (int i = 0; i < 3; i++) {
            dataFlowComponents.add(pipelineDataFlowComponent);
        }

        final Map<String, Object> eventMap = new HashMap<>();
        eventMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final JacksonEvent event = JacksonEvent.builder()
                                       .withData(eventMap)
                                       .withEventType("event")
                                       .build();
        recordsIn = IntStream.range(0, 10)
                .mapToObj(i -> new Record<Event>(event))
                .collect(Collectors.toList());
        final RouterGetRecordStrategy getRecordStrategy = createObjectUnderTest(dataFlowComponents);
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

}
