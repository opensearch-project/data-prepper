/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.s3.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3GroupManagerTest {

    @Mock
    private S3SinkConfig s3SinkConfig;

    @Mock
    private S3GroupIdentifierFactory s3GroupIdentifierFactory;

    @Mock
    private BufferFactory bufferFactory;

    @Mock
    private S3Client s3Client;

    private S3GroupManager createObjectUnderTest() {
        return new S3GroupManager(s3SinkConfig, s3GroupIdentifierFactory, bufferFactory, s3Client);
    }

    @Test
    void hasNoGroups_returns_true_when_there_are_no_groups() {
        final S3GroupManager objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.hasNoGroups(), equalTo(true));
    }

    @Test
    void getOrCreateGroupForEvent_creates_expected_group_when_it_does_not_exist() {
        final Event event = mock(Event.class);
        final S3GroupIdentifier s3GroupIdentifier = mock(S3GroupIdentifier.class);
        when(s3GroupIdentifierFactory.getS3GroupIdentifierForEvent(event)).thenReturn(s3GroupIdentifier);

        final Buffer buffer = mock(Buffer.class);
        when(bufferFactory.getBuffer(eq(s3Client), any(Supplier.class), any(Supplier.class)))
                .thenReturn(buffer);

        final S3GroupManager objectUnderTest = createObjectUnderTest();

        final Map.Entry<S3GroupIdentifier, S3Group> result = objectUnderTest.getOrCreateGroupForEvent(event);

        assertThat(result, notNullValue());
        assertThat(result.getKey(), equalTo(s3GroupIdentifier));
        assertThat(result.getValue(), notNullValue());
        assertThat(result.getValue().getBuffer(), equalTo(buffer));

        final Set<Map.Entry<S3GroupIdentifier, S3Group>> groups = (Set<Map.Entry<S3GroupIdentifier, S3Group>>) objectUnderTest.getS3GroupEntries();
        assertThat(groups, notNullValue());
        assertThat(groups.size(), equalTo(1));

        assertThat(groups.contains(result), equalTo(true));
        assertThat(objectUnderTest.hasNoGroups(), equalTo(false));
    }

    @Test
    void getOrCreateGroupForEvent_returns_expected_group_when_it_exists() {
        final Event event = mock(Event.class);
        final S3GroupIdentifier s3GroupIdentifier = mock(S3GroupIdentifier.class);
        when(s3GroupIdentifierFactory.getS3GroupIdentifierForEvent(event)).thenReturn(s3GroupIdentifier);

        final Buffer buffer = mock(Buffer.class);
        when(bufferFactory.getBuffer(eq(s3Client), any(Supplier.class), any(Supplier.class)))
                .thenReturn(buffer);

        final S3GroupManager objectUnderTest = createObjectUnderTest();

        final Map.Entry<S3GroupIdentifier, S3Group> result = objectUnderTest.getOrCreateGroupForEvent(event);

        assertThat(result, notNullValue());
        assertThat(result.getKey(), equalTo(s3GroupIdentifier));
        assertThat(result.getValue(), notNullValue());
        assertThat(result.getValue().getBuffer(), equalTo(buffer));

        final Map.Entry<S3GroupIdentifier, S3Group> secondResult = objectUnderTest.getOrCreateGroupForEvent(event);

        assertThat(secondResult,  notNullValue());
        assertThat(secondResult, notNullValue());
        assertThat(secondResult.getKey(), equalTo(s3GroupIdentifier));
        assertThat(secondResult.getValue(), notNullValue());
        assertThat(secondResult.getValue().getBuffer(), equalTo(buffer));

        verify(s3GroupIdentifierFactory, times(2)).getS3GroupIdentifierForEvent(event);
        verify(bufferFactory, times(1)).getBuffer(eq(s3Client), any(Supplier.class), any(Supplier.class));

        final Set<Map.Entry<S3GroupIdentifier, S3Group>> groups = (Set<Map.Entry<S3GroupIdentifier, S3Group>>) objectUnderTest.getS3GroupEntries();
        assertThat(groups, notNullValue());
        assertThat(groups.size(), equalTo(1));

        assertThat(groups.contains(result), equalTo(true));
        assertThat(groups.contains(secondResult), equalTo(true));
        assertThat(objectUnderTest.hasNoGroups(), equalTo(false));
    }
}
