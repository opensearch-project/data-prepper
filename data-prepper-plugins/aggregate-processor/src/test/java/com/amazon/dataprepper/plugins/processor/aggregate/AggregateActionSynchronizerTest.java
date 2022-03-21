/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.locks.Lock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AggregateActionSynchronizerTest {

    @Mock
    private AggregateAction aggregateAction;

    @Mock
    private AggregateGroupManager aggregateGroupManager;

    @Mock
    private AggregateGroup aggregateGroup;

    @Mock
    private AggregateIdentificationKeysHasher.IdentificationHash identificationHash;

    @Mock
    private AggregateActionResponse aggregateActionResponse;

    @Mock
    private Lock concludeGroupLock;

    @Mock
    private Lock handleEventForGroupLock;

    @Mock
    private Event event;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter actionHandleEventsProcessingErrors;

    @Mock
    private Counter actionConcludeGroupEventsProcessingErrors;

    @BeforeEach
    void setup() {
        doNothing().when(handleEventForGroupLock).lock();
        doNothing().when(handleEventForGroupLock).unlock();
        doNothing().when(concludeGroupLock).unlock();
        doNothing().when(aggregateGroupManager).putGroupWithHash(identificationHash, aggregateGroup);
        doNothing().when(aggregateGroupManager).closeGroup(identificationHash, aggregateGroup);
        when(aggregateGroupManager.getGroupDuration()).thenReturn(Duration.ZERO);
        when(aggregateGroup.getConcludeGroupLock()).thenReturn(concludeGroupLock);
        when(aggregateGroup.getHandleEventForGroupLock()).thenReturn(handleEventForGroupLock);
        when(aggregateGroup.shouldConcludeGroup(any(Duration.class))).thenReturn(true);

        when(pluginMetrics.counter(AggregateActionSynchronizer.ACTION_HANDLE_EVENTS_PROCESSING_ERRORS)).thenReturn(actionHandleEventsProcessingErrors);
        when(pluginMetrics.counter(AggregateActionSynchronizer.ACTION_CONCLUDE_GROUP_EVENTS_PROCESSING_ERRORS)).thenReturn(actionConcludeGroupEventsProcessingErrors);
    }

    private AggregateActionSynchronizer createObjectUnderTest() {
        final AggregateActionSynchronizer.AggregateActionSynchronizerProvider aggregateActionSynchronizerProvider = new AggregateActionSynchronizer.AggregateActionSynchronizerProvider();
        return aggregateActionSynchronizerProvider.provide(aggregateAction, aggregateGroupManager, pluginMetrics);
    }

    @Test
    void concludeGroup_with_tryLock_false_returns_empty_optional() {
        final AggregateActionSynchronizer objectUnderTest = createObjectUnderTest();
        when(concludeGroupLock.tryLock()).thenReturn(false);

        final Optional<Event> concludeGroupEvent = objectUnderTest.concludeGroup(identificationHash, aggregateGroup);

        verifyNoInteractions(handleEventForGroupLock);
        verifyNoInteractions(aggregateAction);
        verifyNoInteractions(aggregateGroupManager);
        verify(concludeGroupLock, times(0)).unlock();

        assertThat(concludeGroupEvent, equalTo(Optional.empty()));
    }

    @Test
    void concludeGroup_with_tryLock_true_calls_expected_functions_and_returns_correct_event() {
        final AggregateActionSynchronizer objectUnderTest = createObjectUnderTest();
        when(concludeGroupLock.tryLock()).thenReturn(true);
        when(aggregateAction.concludeGroup(aggregateGroup)).thenReturn(Optional.of(event));

        final Optional<Event> concludeGroupEvent = objectUnderTest.concludeGroup(identificationHash, aggregateGroup);

        final InOrder inOrder = Mockito.inOrder(handleEventForGroupLock, aggregateAction, aggregateGroupManager, concludeGroupLock);
        inOrder.verify(handleEventForGroupLock).lock();
        inOrder.verify(aggregateAction).concludeGroup(aggregateGroup);
        inOrder.verify(aggregateGroupManager).closeGroup(identificationHash, aggregateGroup);
        inOrder.verify(handleEventForGroupLock).unlock();
        inOrder.verify(concludeGroupLock).unlock();

        assertThat(concludeGroupEvent.isPresent(), equalTo(true));
        assertThat(concludeGroupEvent.get(), equalTo(event));
    }

    @Test
    void locks_are_unlocked_and_empty_optional_returned_when_aggregateAction_concludeGroup_throws_exception() {
        final AggregateActionSynchronizer objectUnderTest = createObjectUnderTest();
        when(concludeGroupLock.tryLock()).thenReturn(true);
        when(aggregateAction.concludeGroup(aggregateGroup)).thenThrow(RuntimeException.class);

        final Optional<Event> concludeGroupEvent = objectUnderTest.concludeGroup(identificationHash, aggregateGroup);

        final InOrder inOrder = Mockito.inOrder(handleEventForGroupLock, aggregateAction, concludeGroupLock, actionConcludeGroupEventsProcessingErrors);
        inOrder.verify(handleEventForGroupLock).lock();
        inOrder.verify(aggregateAction).concludeGroup(aggregateGroup);
        inOrder.verify(actionConcludeGroupEventsProcessingErrors).increment();
        inOrder.verify(handleEventForGroupLock).unlock();
        inOrder.verify(concludeGroupLock).unlock();

        assertThat(concludeGroupEvent, equalTo(Optional.empty()));
    }

    @Test
    void handleEventForGroup_calls_expected_functions_and_returns_correct_AggregateActionResponse() {
        final AggregateActionSynchronizer objectUnderTest = createObjectUnderTest();
        when(aggregateAction.handleEvent(event, aggregateGroup)).thenReturn(aggregateActionResponse);

        final AggregateActionResponse handleEventResponse = objectUnderTest.handleEventForGroup(event, identificationHash, aggregateGroup);

        final InOrder inOrder = Mockito.inOrder(concludeGroupLock, handleEventForGroupLock, aggregateAction, aggregateGroupManager);
        inOrder.verify(concludeGroupLock).lock();
        inOrder.verify(concludeGroupLock).unlock();
        inOrder.verify(handleEventForGroupLock).lock();
        inOrder.verify(aggregateAction).handleEvent(event, aggregateGroup);
        inOrder.verify(aggregateGroupManager).putGroupWithHash(identificationHash, aggregateGroup);
        inOrder.verify(handleEventForGroupLock).unlock();

        assertThat(handleEventResponse, equalTo(aggregateActionResponse));
    }

    @Test
    void locks_are_unlocked_and_event_returned_when_aggregateAction_handleEvent_throws_exception() {
        final AggregateActionSynchronizer objectUnderTest = createObjectUnderTest();
        when(aggregateAction.handleEvent(event, aggregateGroup)).thenThrow(RuntimeException.class);

        final AggregateActionResponse handleEventResponse = objectUnderTest.handleEventForGroup(event, identificationHash, aggregateGroup);

        final InOrder inOrder = Mockito.inOrder(concludeGroupLock, handleEventForGroupLock, aggregateAction, actionHandleEventsProcessingErrors);
        inOrder.verify(concludeGroupLock).lock();
        inOrder.verify(concludeGroupLock).unlock();
        inOrder.verify(handleEventForGroupLock).lock();
        inOrder.verify(aggregateAction).handleEvent(event, aggregateGroup);
        inOrder.verify(actionHandleEventsProcessingErrors).increment();
        inOrder.verify(handleEventForGroupLock).unlock();

        assertThat(handleEventResponse, notNullValue());
        assertThat(handleEventResponse.getEvent(), equalTo(event));
    }

    @Test
    void conclude_group_with_should_conclude_group_false_returns_empty_optional() {
        final AggregateActionSynchronizer objectUnderTest = createObjectUnderTest();
        when(concludeGroupLock.tryLock()).thenReturn(true);
        when(aggregateGroup.shouldConcludeGroup(any(Duration.class))).thenReturn(false);

        final Optional<Event> concludeGroupEvent = objectUnderTest.concludeGroup(identificationHash, aggregateGroup);

        final InOrder inOrder = Mockito.inOrder(concludeGroupLock, handleEventForGroupLock, aggregateGroup, aggregateGroupManager);
        inOrder.verify(concludeGroupLock).tryLock();
        inOrder.verify(handleEventForGroupLock).lock();
        inOrder.verify(aggregateGroup).shouldConcludeGroup(any(Duration.class));
        inOrder.verify(aggregateGroupManager, times(0)).getGroupDuration();
        inOrder.verify(handleEventForGroupLock).unlock();
        inOrder.verify(concludeGroupLock).unlock();

        verifyNoInteractions(aggregateAction);


        assertThat(concludeGroupEvent, equalTo(Optional.empty()));
    }
}
