package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.sink.s3.codec.BufferedCodec;

import java.io.OutputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CodecBufferTest {
    @Mock
    private Buffer innerBuffer;

    @Mock
    private BufferedCodec bufferedCodec;
    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();
    }

    private CodecBuffer createObjectUnderTest() {
        return new CodecBuffer(innerBuffer, bufferedCodec);
    }

    @Test
    void getSize_returns_BufferedCodec_getSize_if_present() {
        long bufferedCodecSize = Integer.MAX_VALUE + random.nextInt(100_000) + 1000;
        when(bufferedCodec.getSize()).thenReturn(Optional.of(bufferedCodecSize));

        assertThat(createObjectUnderTest().getSize(), equalTo(bufferedCodecSize));

        verify(innerBuffer, never()).getSize();
    }

    @Test
    void getSize_returns_innerBuffer_getSize_if_BufferedCodec_getSize_not_present() {
        long innerSize = Integer.MAX_VALUE + random.nextInt(100_000) + 1000;
        when(innerBuffer.getSize()).thenReturn(innerSize);

        assertThat(createObjectUnderTest().getSize(), equalTo(innerSize));
    }

    @Test
    void getEventCount_returns_inner_getEventCount() {
        int innerEventCount = random.nextInt(100_000) + 1000;
        when(innerBuffer.getEventCount()).thenReturn(innerEventCount);

        assertThat(createObjectUnderTest().getEventCount(), equalTo(innerEventCount));
    }

    @Test
    void getDuration_returns_inner_getDuration() {
        Duration innerDuration = Duration.ofSeconds(random.nextInt(10_000) + 100);
        when(innerBuffer.getDuration()).thenReturn(innerDuration);

        assertThat(createObjectUnderTest().getDuration(), equalTo(innerDuration));

    }

    @Test
    void getOutputStream_returns_inner_getOutputStream() {
        OutputStream innerOutputStream = mock(OutputStream.class);
        when(innerBuffer.getOutputStream()).thenReturn(innerOutputStream);

        assertThat(createObjectUnderTest().getOutputStream(), equalTo(innerOutputStream));
    }

    @Test
    void getKey_returns_inner_getKey() {
        String innerKey = UUID.randomUUID().toString();
        when(innerBuffer.getKey()).thenReturn(innerKey);

        assertThat(createObjectUnderTest().getKey(), equalTo(innerKey));
    }

    @Test
    void setEventCount_calls_inner_setEventCount() {
        int newEventCount = random.nextInt(100_000) + 1000;

        createObjectUnderTest().setEventCount(newEventCount);

        verify(innerBuffer).setEventCount(newEventCount);
    }

    @Test
    void flushToS3_calls_inner_flushToS3() {
        createObjectUnderTest().flushToS3();

        verify(innerBuffer).flushToS3();
    }
}