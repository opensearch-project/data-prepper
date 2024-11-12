/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.peerforwarder.exception.EmptyPeerForwarderPluginIdentificationKeysException;
import org.opensearch.dataprepper.core.peerforwarder.exception.UnsupportedPeerForwarderPluginException;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PeerForwardingProcessingDecoratorTest {
    private static final int PIPELINE_WORKER_THREADS = new Random().nextInt(10) + 1;
    private static final String TEST_IDENTIFICATION_KEY = "identification_key";

    private Record<Event> record;

    @Mock
    private Processor processor;

    @Mock(extraInterfaces = Processor.class)
    private RequiresPeerForwarding requiresPeerForwarding;

    @Mock(extraInterfaces = Processor.class)
    private RequiresPeerForwarding requiresPeerForwardingCopy;

    @Mock
    private PeerForwarderProvider peerForwarderProvider;

    private String pipelineName;
    private String pluginId;

    @BeforeEach
    void setUp() {
        record = mock(Record.class);
        pipelineName = UUID.randomUUID().toString();
        pluginId = UUID.randomUUID().toString();
    }

    private List<Processor> createObjectUnderTestDecoratedProcessors(final List<Processor> processors) {
        return PeerForwardingProcessorDecorator.decorateProcessors(processors, peerForwarderProvider, pipelineName, pluginId, null, PIPELINE_WORKER_THREADS);
    }

    private List<Processor> createObjectUnderTestDecoratedProcessorsWithExcludeIdentificationKeys(final List<Processor> processors, Set<Set<String>> excludeIdentificationKeys) {
        return PeerForwardingProcessorDecorator.decorateProcessors(processors, peerForwarderProvider, pipelineName, pluginId, excludeIdentificationKeys, PIPELINE_WORKER_THREADS);
    }

    @Test
    void PeerForwardingProcessingDecorator_should_not_have_any_interactions_if_its_not_an_instance_of_RequiresPeerForwarding() {
        assertThrows(UnsupportedPeerForwarderPluginException.class, () -> createObjectUnderTestDecoratedProcessors(Collections.singletonList(processor)));

        verifyNoInteractions(peerForwarderProvider);
    }

    @Test
    void PeerForwardingProcessingDecorator_execute_with_empty_identification_keys_should_throw() {
        when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(Collections.emptySet());

        assertThrows(EmptyPeerForwarderPluginIdentificationKeysException.class, () -> createObjectUnderTestDecoratedProcessors(Collections.singletonList((Processor) requiresPeerForwarding)));
    }

    @Test
    void decorateProcessors_with_different_identification_key_should_throw() {
        List<Processor> processorList = new ArrayList<>();
        processorList.add((Processor) requiresPeerForwarding);
        processorList.add((Processor) requiresPeerForwardingCopy);

        when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(Set.of(UUID.randomUUID().toString()));
        when(requiresPeerForwardingCopy.getIdentificationKeys()).thenReturn(Set.of(UUID.randomUUID().toString()));

        assertThrows(RuntimeException.class, () -> createObjectUnderTestDecoratedProcessors(List.of(((Processor) requiresPeerForwarding), (Processor) requiresPeerForwardingCopy)));
    }

    @Test
    void decorateProcessors_with_excludeIdentificationKeys() {
        Set<String> identificationKeys = Set.of("key1", "key2");
        when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(identificationKeys);
        final List<Processor> processors = createObjectUnderTestDecoratedProcessorsWithExcludeIdentificationKeys(Collections.singletonList((Processor) requiresPeerForwarding), Set.of(identificationKeys));
        assertThat(processors.size(), equalTo(1));
        for (final Processor processor: processors) {
            assertTrue(((PeerForwardingProcessorDecorator)processor).isPeerForwardingDisabled());
        }
    }

    @Test
    void decorateProcessors_with_notmatching_excludeIdentificationKeys() {
        Set<String> identificationKeys = Set.of("key1", "key2");
        Set<String> notMatchingIdentificationKeys = Set.of("key1", "key3");
        when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(identificationKeys);
        final List<Processor> processors = createObjectUnderTestDecoratedProcessorsWithExcludeIdentificationKeys(Collections.singletonList((Processor) requiresPeerForwarding), Set.of(notMatchingIdentificationKeys));
        assertThat(processors.size(), equalTo(1));
        for (final Processor processor: processors) {
            assertFalse(((PeerForwardingProcessorDecorator)processor).isPeerForwardingDisabled());
        }
    }

    @Test
    void decorateProcessors_with_empty_processors_should_return_empty_list_of_processors() {
        final List<Processor> processors = createObjectUnderTestDecoratedProcessors(Collections.emptyList());
        assertThat(processors.size(), equalTo(0));
    }

    @Nested
    class WithRegisteredPeerForwarder {
        @Mock
        private RemotePeerForwarder peerForwarder;
        private Set<String> identificationKeys;
        private Processor processor;

        @BeforeEach
        void setUp() {
            identificationKeys = Set.of(TEST_IDENTIFICATION_KEY);

            processor = (Processor) requiresPeerForwarding;
            lenient().when(peerForwarderProvider.register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS)).thenReturn(peerForwarder);
            when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(identificationKeys);
        }

        @Test
        void PeerForwardingProcessingDecorator_should_have_interaction_with_getIdentificationKeys() {
            createObjectUnderTestDecoratedProcessors(Collections.singletonList(processor));
            verify(requiresPeerForwarding, times(2)).getIdentificationKeys();
            verify(peerForwarderProvider).register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);
            verifyNoMoreInteractions(peerForwarderProvider);
        }

        @Test
        void PeerForwardingProcessingDecorator_should_have_interaction_with_getIdentificationKeys_when_list_of_processors() {
            when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(identificationKeys);
            when(requiresPeerForwardingCopy.getIdentificationKeys()).thenReturn(identificationKeys);

            createObjectUnderTestDecoratedProcessors(List.of((Processor) requiresPeerForwarding, (Processor) requiresPeerForwardingCopy));

            verify(requiresPeerForwarding, times(2)).getIdentificationKeys();
            verify(peerForwarderProvider).register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);
            verifyNoMoreInteractions(peerForwarderProvider);
        }

        @Test
        void PeerForwardingProcessingDecorator_with_localProcessingOnlyWithExcludeIdentificationKeys() {
            List<Processor> objectsUnderTest = new ArrayList<>();
            Processor innerProcessor1 = (Processor)requiresPeerForwarding;
            Processor innerProcessor2 = (Processor)requiresPeerForwardingCopy;
            objectsUnderTest.add(innerProcessor1);
            objectsUnderTest.add(innerProcessor2);

            LocalPeerForwarder localPeerForwarder = mock(LocalPeerForwarder.class);
            when(peerForwarderProvider.register(pipelineName, (Processor) requiresPeerForwarding, pluginId, identificationKeys, PIPELINE_WORKER_THREADS)).thenReturn(localPeerForwarder);
            Event event = mock(Event.class);
            when(record.getData()).thenReturn(event);
            List<Record<Event>> testData = Collections.singletonList(record);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event)).thenReturn(true);
            when(requiresPeerForwardingCopy.isApplicableEventForPeerForwarding(event)).thenReturn(true);

            when(innerProcessor1.execute(testData)).thenReturn(testData);
            when(innerProcessor2.execute(testData)).thenReturn(testData);

            when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(identificationKeys);
            when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(identificationKeys);
            when(requiresPeerForwardingCopy.getIdentificationKeys()).thenReturn(identificationKeys);

            final List<Processor> processors = createObjectUnderTestDecoratedProcessorsWithExcludeIdentificationKeys(objectsUnderTest, Set.of(identificationKeys));
            assertThat(processors.size(), equalTo(2));
            for (final Processor processor: processors) {
                assertTrue(((PeerForwardingProcessorDecorator)processor).isPeerForwardingDisabled());
            }
            verify(peerForwarderProvider, times(1)).register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);
            verifyNoMoreInteractions(peerForwarderProvider);
            Collection<Record<Event>> result = processors.get(0).execute(testData);
            assertThat(result.size(), equalTo(testData.size()));
            assertThat(result, equalTo(testData));
            result = processors.get(1).execute(testData);
            assertThat(result.size(), equalTo(testData.size()));
            assertThat(result, equalTo(testData));
        }

 
        @Test
        void PeerForwardingProcessingDecorator_with_localProcessingOnly() {
            List<Processor> processorList = new ArrayList<>();
            processorList.add((Processor) requiresPeerForwarding);
            processorList.add((Processor) requiresPeerForwardingCopy);

            LocalPeerForwarder localPeerForwarder = mock(LocalPeerForwarder.class);
            when(peerForwarderProvider.register(pipelineName, (Processor) requiresPeerForwarding, pluginId, identificationKeys, PIPELINE_WORKER_THREADS)).thenReturn(localPeerForwarder);
            Event event = mock(Event.class);
            when(record.getData()).thenReturn(event);
            List<Record<Event>> testData = Collections.singletonList(record);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event)).thenReturn(false);
            when(requiresPeerForwardingCopy.isApplicableEventForPeerForwarding(event)).thenReturn(false);

            Processor processor1 = (Processor)requiresPeerForwarding;
            Processor processor2 = (Processor)requiresPeerForwardingCopy;
            when(processor1.execute(testData)).thenReturn(testData);
            when(processor2.execute(testData)).thenReturn(testData);

            when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(identificationKeys);
            when(requiresPeerForwardingCopy.getIdentificationKeys()).thenReturn(identificationKeys);

            when(requiresPeerForwarding.isForLocalProcessingOnly(any())).thenReturn(true);
            when(requiresPeerForwardingCopy.isForLocalProcessingOnly(any())).thenReturn(true);

            final List<Processor> processors = createObjectUnderTestDecoratedProcessors(processorList);
            assertThat(processors.size(), equalTo(2));
            for (final Processor processor: processors) {
                assertFalse(((PeerForwardingProcessorDecorator)processor).isPeerForwardingDisabled());
            }
            verify(peerForwarderProvider, times(1)).register(pipelineName, processor, pluginId, identificationKeys, PIPELINE_WORKER_THREADS);
            verifyNoMoreInteractions(peerForwarderProvider);
            Collection<Record<Event>> result = processors.get(0).execute(testData);
            assertThat(result.size(), equalTo(testData.size()));
            assertThat(result, equalTo(testData));
            result = processors.get(1).execute(testData);
            assertThat(result.size(), equalTo(testData.size()));
            assertThat(result, equalTo(testData));
        }

        @Test
        void PeerForwardingProcessingDecorator_execute_should_forwardRecords_with_correct_values() {
            Event event = mock(Event.class);
            when(record.getData()).thenReturn(event);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event)).thenReturn(true);
            List<Record<Event>> testData = Collections.singletonList(record);

            when(peerForwarder.forwardRecords(anyCollection())).thenReturn(testData);

            when(processor.execute(testData)).thenReturn(testData);

            final List<Processor> processors = createObjectUnderTestDecoratedProcessors(Collections.singletonList(processor));
            assertThat(processors.size(), equalTo(1));
            final Collection<Record<Event>> records = processors.get(0).execute(testData);

            verify(requiresPeerForwarding, times(2)).getIdentificationKeys();
            verify(peerForwarder).forwardRecords(anyCollection());
            Assertions.assertNotNull(records);
            assertThat(records.size(), equalTo(testData.size()));
            assertThat(records, equalTo(testData));
        }

        @Test
        void PeerForwardingProcessingDecorator_execute_should_receiveRecords() {
            Event event = mock(Event.class);
            when(record.getData()).thenReturn(event);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event)).thenReturn(true);
            Collection<Record<Event>> forwardTestData = Collections.singletonList(record);
            Collection<Record<Event>> receiveTestData = Collections.singletonList(mock(Record.class));

            when(peerForwarder.forwardRecords(anyCollection())).thenReturn(forwardTestData);
            when(peerForwarder.receiveRecords()).thenReturn(receiveTestData);

            final Collection<Record<Event>> expectedRecordsToProcessLocally = CollectionUtils.union(forwardTestData, receiveTestData);

            when(((Processor) requiresPeerForwarding).execute(anyCollection())).thenReturn(expectedRecordsToProcessLocally);

            final List<Processor> processors = createObjectUnderTestDecoratedProcessors(Collections.singletonList((Processor) requiresPeerForwarding));
            assertThat(processors.size(), equalTo(1));
            for (final Processor processor: processors) {
                assertFalse(((PeerForwardingProcessorDecorator)processor).isPeerForwardingDisabled());
            }
            final Collection<Record<Event>> records = processors.get(0).execute(forwardTestData);

            verify(requiresPeerForwarding, times(2)).getIdentificationKeys();
            verify(peerForwarder).forwardRecords(anyCollection());
            verify(peerForwarder).receiveRecords();
            Assertions.assertNotNull(records);
            assertThat(records.size(), equalTo(expectedRecordsToProcessLocally.size()));
            assertThat(records, equalTo(expectedRecordsToProcessLocally));
        }

        @Test
        void PeerForwardingProcessingDecorator_execute_will_call_inner_processors_execute() {
            Event event = mock(Event.class);
            when(record.getData()).thenReturn(event);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event)).thenReturn(true);
            final List<Processor> processors = createObjectUnderTestDecoratedProcessors(Collections.singletonList(processor));
            Collection<Record<Event>> testData = Collections.singletonList(record);

            assertThat(processors.size(), equalTo(1));
            processors.get(0).execute(testData);
            verify(processor).execute(anyCollection());
        }


        @Test
        void PeerForwardingProcessingDecorator_execute_will_call_inner_processors_execute_with_local_processing() {
            Event event = mock(Event.class);
            when(record.getData()).thenReturn(event);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event)).thenReturn(false);
            when(requiresPeerForwarding.isForLocalProcessingOnly(any())).thenReturn(true);

            final List<Processor> processors = createObjectUnderTestDecoratedProcessors(Collections.singletonList(processor));
            Collection<Record<Event>> testData = Collections.singletonList(record);

            assertThat(processors.size(), equalTo(1));
            processors.get(0).execute(testData);
            verify(processor).execute(anyCollection());
        }

        @Test
        void PeerForwardingProcessingDecorator_inner_processor_with_is_applicable_event_and_local_processing() {
            Event event1 = mock(Event.class);
            Event event2 = mock(Event.class);
            Event event3 = mock(Event.class);
            Record record1 = mock(Record.class);
            Record record2 = mock(Record.class);
            Record record3 = mock(Record.class);
            Record aggregatedRecord = mock(Record.class);
            List<Record> aggregatedRecords = new ArrayList<>();
            aggregatedRecords.add(aggregatedRecord);
            when(processor.execute(anyCollection())).thenReturn(aggregatedRecords);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event1)).thenReturn(false);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event2)).thenReturn(false);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event3)).thenReturn(false);
            when(requiresPeerForwarding.isForLocalProcessingOnly(any())).thenReturn(true);
            final List<Processor> processors = createObjectUnderTestDecoratedProcessors(Collections.singletonList(processor));
            when(record1.getData()).thenReturn(event1);
            when(record2.getData()).thenReturn(event2);
            when(record3.getData()).thenReturn(event3);
            Collection<Record<Event>> recordsIn = new ArrayList<>();
            recordsIn.add(record1);
            recordsIn.add(record2);
            recordsIn.add(record3);

            assertThat(processors.size(), equalTo(1));
            Collection<Record<Event>> recordsOut = processors.get(0).execute(recordsIn);
            verify(processor).execute(anyCollection());
            assertThat(recordsOut.size(), equalTo(1));
            assertTrue(recordsOut.contains(aggregatedRecord));
        }

        @Test
        void PeerForwardingProcessingDecorator_inner_processor_with_is_applicable_event_overridden() {
            Event event1 = mock(Event.class);
            Event event2 = mock(Event.class);
            Event event3 = mock(Event.class);
            Record record1 = mock(Record.class);
            Record record2 = mock(Record.class);
            Record record3 = mock(Record.class);
            Record aggregatedRecord = mock(Record.class);
            List<Record> aggregatedRecords = new ArrayList<>();
            aggregatedRecords.add(aggregatedRecord);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event1)).thenReturn(true);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event2)).thenReturn(false);
            when(requiresPeerForwarding.isApplicableEventForPeerForwarding(event3)).thenReturn(true);
            when(requiresPeerForwarding.isForLocalProcessingOnly(any())).thenReturn(false);
            final List<Processor> processors = createObjectUnderTestDecoratedProcessors(Collections.singletonList(processor));
            when(record1.getData()).thenReturn(event1);
            when(record2.getData()).thenReturn(event2);
            when(record3.getData()).thenReturn(event3);
            Collection<Record<Event>> recordsIn = new ArrayList<>();
            recordsIn.add(record1);
            recordsIn.add(record2);
            recordsIn.add(record3);

            assertThat(processors.size(), equalTo(1));
            Collection<Record<Event>> recordsOut = processors.get(0).execute(recordsIn);
            verify(processor).execute(anyCollection());
            // one record skipped, no records locally processed
            assertThat(recordsOut.size(), equalTo(1));
            assertTrue(recordsOut.contains(record2));
        }

        @Test
        void PeerForwardingProcessingDecorator_prepareForShutdown_will_call_inner_processors_prepareForShutdown() {
            final List<Processor> processors = createObjectUnderTestDecoratedProcessors(Collections.singletonList(processor));

            assertThat(processors.size(), equalTo(1));
            processors.get(0).prepareForShutdown();
            verify(processor).prepareForShutdown();
        }

        @Test
        void PeerForwardingProcessingDecorator_isReadyForShutdown_will_call_inner_processors_isReadyForShutdown() {
            final List<Processor> processors = createObjectUnderTestDecoratedProcessors(Collections.singletonList(processor));

            assertThat(processors.size(), equalTo(1));
            processors.get(0).isReadyForShutdown();
            verify(processor).isReadyForShutdown();
        }

        @Test
        void PeerForwardingProcessingDecorator_shutdown_will_call_inner_processors_shutdown() {
            final List<Processor> processors = createObjectUnderTestDecoratedProcessors(Collections.singletonList(processor));

            assertThat(processors.size(), equalTo(1));
            processors.get(0).shutdown();
            verify(processor).shutdown();
        }
    }

}
