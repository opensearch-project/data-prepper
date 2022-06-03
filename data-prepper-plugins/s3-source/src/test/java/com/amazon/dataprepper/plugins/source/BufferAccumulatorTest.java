/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class BufferAccumulatorTest {
    @Mock
    private Buffer<Record<?>> buffer;
    private int recordsToAccumulate;
    private Duration bufferTimeout;
    private int timeoutMillis;

    @BeforeEach
    void setUp() {
        recordsToAccumulate = 20;
        timeoutMillis = 100;
        bufferTimeout = Duration.ofMillis(timeoutMillis);
    }

    private BufferAccumulator createObjectUnderTest() {
        return BufferAccumulator.create(buffer, recordsToAccumulate, bufferTimeout);
    }

    @Test
    void constructor_should_throw_if_buffer_is_null() {
        buffer = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, -2, Integer.MIN_VALUE})
    void constructor_should_throw_if_numberOfRecordsToAccumulate_is_not_positive(int nonPositiveNumber) {
        recordsToAccumulate = nonPositiveNumber;
        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_should_throw_if_bufferTimeout_is_null() {
        bufferTimeout = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 10, 20})
    void add_should_not_write_to_Buffer(final int accumulationCount) throws Exception {
        recordsToAccumulate = accumulationCount;

        createObjectUnderTest().add(createRecord());
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 10, 20})
    void adding_to_accumulation_count_should_write_to_Buffer(final int accumulationCount) throws Exception {
        recordsToAccumulate = accumulationCount;

        final BufferAccumulator objectUnderTest = createObjectUnderTest();
        final List<Record<?>> knownRecords = new ArrayList<>();
        for (int i = 0; i < accumulationCount - 1; i++) {
            final Record record = createRecord();
            knownRecords.add(record);
            objectUnderTest.add(record);
        }
        verifyNoInteractions(buffer);

        final Collection<Record<?>> actualRecordsWritten = new ArrayList<>();
        doAnswer(a -> actualRecordsWritten.addAll(a.getArgument(0, Collection.class)))
                .when(buffer).writeAll(anyCollection(), anyInt());

        final Record record = createRecord();
        knownRecords.add(record);
        objectUnderTest.add(record);

        verify(buffer).writeAll(anyCollection(), eq(timeoutMillis));

        assertThat(actualRecordsWritten.size(), equalTo(accumulationCount));
        assertThat(actualRecordsWritten, equalTo(knownRecords));
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 10, 20})
    void adding_past_accumulation_count_should_write_to_Buffer_every_accumulation_count(final int accumulationCount) throws Exception {
        recordsToAccumulate = accumulationCount;

        final BufferAccumulator objectUnderTest = createObjectUnderTest();
        final int expectedWrites = 37;

        final Set<Record<?>> actualRecordsWritten = new HashSet<>();
        doAnswer(a -> actualRecordsWritten.addAll(a.getArgument(0, Collection.class)))
                .when(buffer).writeAll(anyCollection(), anyInt());

        final int totalRecordsToWrite = accumulationCount * 37;
        for (int i = 0; i < totalRecordsToWrite; i++) {
            objectUnderTest.add(createRecord());
        }
        verify(buffer, times(expectedWrites)).writeAll(anyCollection(), eq(timeoutMillis));

        assertThat(actualRecordsWritten.size(), equalTo(totalRecordsToWrite));
    }

    @Test
    void flush_on_new_buffer_will_not_write() throws Exception {
        createObjectUnderTest().flush();

        verifyNoInteractions(buffer);
    }

    @Test
    void flush_after_add_writes_to_buffer() throws Exception {
        final BufferAccumulator objectUnderTest = createObjectUnderTest();
        final Record record = createRecord();
        objectUnderTest.add(record);

        verifyNoInteractions(buffer);

        final List<Record<?>> actualRecordsWritten = new ArrayList<>();
        doAnswer(a -> actualRecordsWritten.addAll(a.getArgument(0, Collection.class)))
                .when(buffer).writeAll(anyCollection(), anyInt());

        objectUnderTest.flush();

        verify(buffer).writeAll(anyCollection(), eq(timeoutMillis));

        assertThat(actualRecordsWritten.size(), equalTo(1));
        assertThat(actualRecordsWritten, equalTo(Collections.singletonList(record)));
    }

    private Record createRecord() {
        return mock(Record.class);
    }
}