package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.Buffer;

import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
}
