package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.Buffer;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3GroupTest {

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void releasingEventHandles_releases_all_event_handles(final boolean result) {
        final S3GroupIdentifier s3GroupIdentifier = mock(S3GroupIdentifier.class);
        final Buffer buffer = mock(Buffer.class);
        final S3Group objectUnderTest = new S3Group(s3GroupIdentifier, buffer);
        final Collection<EventHandle> eventHandles = List.of(mock(EventHandle.class), mock(EventHandle.class));

        for (final EventHandle eventHandle : eventHandles) {
            objectUnderTest.addEventHandle(eventHandle);
        }

        objectUnderTest.releaseEventHandles(result);

        for (final EventHandle eventHandle : eventHandles) {
            verify(eventHandle).release(result);
        }
    }

    @Test
    void comparingS3GroupsReturns_expected_result_based_on_buffer_size() {
        final Buffer smallBuffer = mock(Buffer.class);
        when(smallBuffer.getSize()).thenReturn(100L);

        final Buffer largeBuffer = mock(Buffer.class);
        when(largeBuffer.getSize()).thenReturn(1000L);

        final Buffer equalBuffer = mock(Buffer.class);
        when(equalBuffer.getSize()).thenReturn(1000L);

        final S3Group smallGroup = new S3Group(mock(S3GroupIdentifier.class), smallBuffer);
        final S3Group largeGroup = new S3Group(mock(S3GroupIdentifier.class), largeBuffer);
        final S3Group anotherLargeGroup = new S3Group(mock(S3GroupIdentifier.class), equalBuffer);

        assertThat(smallGroup.compareTo(largeGroup), equalTo(1));
        assertThat(largeGroup.compareTo(smallGroup), equalTo(-1));
        assertThat(largeGroup.compareTo(anotherLargeGroup), equalTo(0));
    }
}
