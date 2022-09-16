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

    @Mock(extraInterfaces = Processor.class)
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

        assertThrows(EmptyPeerForwarderPluginIdentificationKeysException.class, () -> createObjectUnderTest((Processor) requiresPeerForwarding));
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

            when(peerForwarderProvider.register(pipelineName, pluginId, identificationKeys)).thenReturn(peerForwarder);
            when(requiresPeerForwarding.getIdentificationKeys()).thenReturn(identificationKeys);
            processor = (Processor) requiresPeerForwarding;
        }

        @Test
        void PeerForwardingProcessingDecorator_should_have_interaction_with_getIdentificationKeys() {
            createObjectUnderTest(processor);
            verify(requiresPeerForwarding).getIdentificationKeys();
            verify(peerForwarderProvider).register(pipelineName, pluginId, identificationKeys);
        }

        @Test
        void PeerForwardingProcessingDecorator_execute_should_forwardRecords_with_correct_values() {
            List<Record<Event>> testData = Collections.singletonList(record);

            when(peerForwarder.forwardRecords(testData)).thenReturn(testData);

            when(processor.execute(testData)).thenReturn(testData);

            final PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest(processor);
            final Collection<Record<Event>> records = objectUnderTest.execute(testData);

            verify(requiresPeerForwarding).getIdentificationKeys();
            verify(peerForwarder).forwardRecords(testData);
            Assertions.assertNotNull(records);
            assertThat(records.size(), equalTo(testData.size()));
            assertThat(records, equalTo(testData));
        }

        @Test
        void PeerForwardingProcessingDecorator_execute_will_call_inner_processors_execute() {
            PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest(processor);
            Collection<Record<Event>> testData = Collections.singletonList(record);
            objectUnderTest.execute(testData);
            verify(processor).execute(anyCollection());
        }

        @Test
        void PeerForwardingProcessingDecorator_prepareForShutdown_will_call_inner_processors_prepareForShutdown() {
            PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest(processor);

            objectUnderTest.prepareForShutdown();
            verify(processor).prepareForShutdown();
        }

        @Test
        void PeerForwardingProcessingDecorator_isReadyForShutdown_will_call_inner_processors_isReadyForShutdown() {
            PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest(processor);

            objectUnderTest.isReadyForShutdown();
            verify(processor).isReadyForShutdown();
        }

        @Test
        void PeerForwardingProcessingDecorator_shutdown_will_call_inner_processors_shutdown() {
            PeerForwardingProcessorDecorator objectUnderTest = createObjectUnderTest(processor);

            objectUnderTest.shutdown();
            verify(processor).shutdown();
        }
    }

}