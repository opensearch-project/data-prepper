/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.apache.commons.collections.CollectionUtils;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.peerforwarder.exception.EmptyPeerForwarderPluginIdentificationKeysException;
import org.opensearch.dataprepper.peerforwarder.exception.UnsupportedPeerForwarderPluginException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PeerForwardingProcessingDecoratorTest {
    private static final String TEST_IDENTIFICATION_KEY = "identification_key";

    private Record<Event> record;

    @Mock
    private Processor processor;

    @Mock
    private RequiresPeerForwarding requiresPeerForwarding;

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

    private PeerForwardingProcessorDecorator createObjectUnderTest(final Processor processor) {
        return new PeerForwardingProcessorDecorator(processor, peerForwarderProvider, pipelineName, pluginId);
    }

    @Test
    void PeerForwardingProcessingDecorator_should_not_have_any_interactions_if_its_not_an_instance_of_RequiresPeerForwarding() {
        assertThrows(UnsupportedPeerForwarderPluginException.class, () -> createObjectUnderTest(processor));

        verifyNoInteractions(peerForwarderProvider);
    }

    @Test
    void PeerForwardingProcessingDecorator_execute_with_empty_identification_keys_should_throw() {
        when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(Collections.emptySet());

        assertThrows(EmptyPeerForwarderPluginIdentificationKeysException.class, () -> createObjectUnderTest(requiresPeerForwarding));
    }

    @Nested
    class WithRegisteredPeerForwarder {
        @Mock
        private RemotePeerForwarder peerForwarder;
        private Set<String> identificationKeys;

        @BeforeEach
        void setUp() {
            identificationKeys = Set.of(TEST_IDENTIFICATION_KEY);

            when(peerForwarderProvider.register(pipelineName, pluginId, identificationKeys)).thenReturn(peerForwarder);
            when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(identificationKeys);
        }

        @Test
        void PeerForwardingProcessingDecorator_should_have_interaction_with_getIdentificationKeys() {
            createObjectUnderTest(requiresPeerForwarding);
            verify(requiresPeerForwarding).getIdentificationKeys();
            verify(peerForwarderProvider).register(pipelineName, pluginId, identificationKeys);
        }

        @Test
        void PeerForwardingProcessingDecorator_execute_should_forwardRecords_with_correct_values() {
            List<Record<Event>> testData = Collections.singletonList(record);

            when(peerForwarder.forwardRecords(testData)).thenReturn(testData);

            when(requiresPeerForwarding.execute(testData)).thenReturn(testData);

            final PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest(requiresPeerForwarding);
            final Collection<Record<Event>> records = objectUnderTest.execute(testData);

            verify(requiresPeerForwarding).getIdentificationKeys();
            verify(peerForwarder).forwardRecords(testData);
            Assertions.assertNotNull(records);
            assertThat(records.size(), equalTo(testData.size()));
            assertThat(records, equalTo(testData));
        }

        @Test
        void PeerForwardingProcessingDecorator_execute_should_receiveRecords() {
            Collection<Record<Event>> forwardTestData = Collections.singletonList(record);
            Collection<Record<Event>> receiveTestData = Collections.singletonList(mock(Record.class));

            when(peerForwarder.forwardRecords(forwardTestData)).thenReturn(forwardTestData);
            when(peerForwarder.receiveRecords()).thenReturn(receiveTestData);

            final Collection<Record<Event>> recordsToProcessLocally = CollectionUtils.union(forwardTestData, receiveTestData);

            when(requiresPeerForwarding.execute(anyCollection())).thenReturn(recordsToProcessLocally);

            final PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest(requiresPeerForwarding);
            final Collection<Record<Event>> records = objectUnderTest.execute(forwardTestData);

            verify(requiresPeerForwarding).getIdentificationKeys();
            verify(peerForwarder).forwardRecords(forwardTestData);
            verify(peerForwarder).receiveRecords();
            Assertions.assertNotNull(records);
            assertThat(records.size(), equalTo(recordsToProcessLocally.size()));
            assertThat(records, equalTo(recordsToProcessLocally));
        }

        @Test
        void PeerForwardingProcessingDecorator_execute_will_call_inner_processors_execute() {
            PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest(requiresPeerForwarding);
            Collection<Record<Event>> testData = Collections.singletonList(record);
            objectUnderTest.execute(testData);
            verify(requiresPeerForwarding).execute(anyCollection());
        }

        @Test
        void PeerForwardingProcessingDecorator_prepareForShutdown_will_call_inner_processors_prepareForShutdown() {
            PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest(requiresPeerForwarding);

            objectUnderTest.prepareForShutdown();
            verify(requiresPeerForwarding).prepareForShutdown();
        }

        @Test
        void PeerForwardingProcessingDecorator_isReadyForShutdown_will_call_inner_processors_isReadyForShutdown() {
            PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest(requiresPeerForwarding);

            objectUnderTest.isReadyForShutdown();
            verify(requiresPeerForwarding).isReadyForShutdown();
        }

        @Test
        void PeerForwardingProcessingDecorator_shutdown_will_call_inner_processors_shutdown() {
            PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest(requiresPeerForwarding);

            objectUnderTest.shutdown();
            verify(requiresPeerForwarding).shutdown();
        }
    }

}