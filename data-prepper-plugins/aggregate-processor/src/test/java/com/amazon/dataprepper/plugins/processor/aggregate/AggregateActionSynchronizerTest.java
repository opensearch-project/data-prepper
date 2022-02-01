/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.model.event.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.concurrent.locks.Lock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
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

    @BeforeEach
    void setup() {
        lenient().doNothing().when(handleEventForGroupLock).lock();
        lenient().doNothing().when(handleEventForGroupLock).unlock();
        lenient().doNothing().when(concludeGroupLock).unlock();
        lenient().doNothing().when(aggregateGroup).clearGroupState();
        lenient().doNothing().when(aggregateGroup).resetGroupStart();
        lenient().doNothing().when(aggregateGroupManager).putGroupWithHash(identificationHash, aggregateGroup);
        lenient().doNothing().when(aggregateGroupManager).removeGroupWithHash(identificationHash, aggregateGroup);
        when(aggregateGroup.getConcludeGroupLock()).thenReturn(concludeGroupLock);
        when(aggregateGroup.getHandleEventForGroupLock()).thenReturn(handleEventForGroupLock);
    }

    private AggregateActionSynchronizer createObjectUnderTest() {
        final AggregateActionSynchronizer.AggregateActionSynchronizerProvider aggregateActionSynchronizerProvider = new AggregateActionSynchronizer.AggregateActionSynchronizerProvider();
        return aggregateActionSynchronizerProvider.provide(aggregateAction, aggregateGroupManager);
    }

    @Test
    void concludeGroup_with_tryLock_false_returns_empty_optional() {
        final AggregateActionSynchronizer objectUnderTest = createObjectUnderTest();
        when(concludeGroupLock.tryLock()).thenReturn(false);

        final Optional<Event> concludeGroupEvent = objectUnderTest.concludeGroup(identificationHash, aggregateGroup);

        verifyNoInteractions(handleEventForGroupLock);
        verifyNoInteractions(aggregateAction);
        verifyNoInteractions(aggregateGroupManager);
        verify(aggregateGroup, times(0)).resetGroupStart();
        verify(aggregateGroup, times(0)).clearGroupState();
        verify(concludeGroupLock, times(0)).unlock();

        assertThat(concludeGroupEvent, equalTo(Optional.empty()));
    }

    @Test
    void concludeGroup_with_tryLock_true_calls_expected_functions_and_returns_correct_event() {
        final AggregateActionSynchronizer objectUnderTest = createObjectUnderTest();
        when(concludeGroupLock.tryLock()).thenReturn(true);
        lenient().when(aggregateAction.concludeGroup(aggregateGroup)).thenReturn(Optional.of(event));

        final Optional<Event> concludeGroupEvent = objectUnderTest.concludeGroup(identificationHash, aggregateGroup);

        verify(handleEventForGroupLock).lock();
        verify(handleEventForGroupLock).unlock();
        verify(aggregateGroup).resetGroupStart();
        verify(aggregateGroup).clearGroupState();
        verify(aggregateGroupManager).removeGroupWithHash(identificationHash, aggregateGroup);
        verify(aggregateAction).concludeGroup(aggregateGroup);
        verify(concludeGroupLock).unlock();

        assertThat(concludeGroupEvent.isPresent(), equalTo(true));
        assertThat(concludeGroupEvent.get(), equalTo(event));
    }

    @Test
    void locks_are_unlocked_and_empty_optional_returned_when_aggregateAction_concludeGroup_throws_exception() {
        final AggregateActionSynchronizer objectUnderTest = createObjectUnderTest();
        when(concludeGroupLock.tryLock()).thenReturn(true);
        lenient().when(aggregateAction.concludeGroup(aggregateGroup)).thenThrow(RuntimeException.class);

        final Optional<Event> concludeGroupEvent = objectUnderTest.concludeGroup(identificationHash, aggregateGroup);

        verify(handleEventForGroupLock).unlock();
        verify(concludeGroupLock).unlock();

        assertThat(concludeGroupEvent, equalTo(Optional.empty()));
    }

    @Test
    void handleEventForGroup_calls_expected_functions_and_returns_correct_AggregateActionResponse() {
        final AggregateActionSynchronizer objectUnderTest = createObjectUnderTest();
        lenient().when(aggregateAction.handleEvent(event, aggregateGroup)).thenReturn(aggregateActionResponse);

        final AggregateActionResponse handleEventResponse = objectUnderTest.handleEventForGroup(event, identificationHash, aggregateGroup);

        verify(concludeGroupLock).lock();
        verify(concludeGroupLock).unlock();
        verify(handleEventForGroupLock).lock();
        verify(handleEventForGroupLock).unlock();
        verify(aggregateGroupManager).putGroupWithHash(identificationHash, aggregateGroup);
        verify(aggregateAction).handleEvent(event, aggregateGroup);

        assertThat(handleEventResponse, equalTo(aggregateActionResponse));
    }

    @Test
    void locks_are_unlocked_and_event_returned_when_aggregateAction_handleEvent_throws_exception() {
        final AggregateActionSynchronizer objectUnderTest = createObjectUnderTest();
        lenient().when(aggregateAction.handleEvent(event, aggregateGroup)).thenThrow(RuntimeException.class);

        final AggregateActionResponse handleEventResponse = objectUnderTest.handleEventForGroup(event, identificationHash, aggregateGroup);

        verify(handleEventForGroupLock).unlock();

        assertThat(handleEventResponse, notNullValue());
        assertThat(handleEventResponse.getEvent(), equalTo(event));
    }

}
