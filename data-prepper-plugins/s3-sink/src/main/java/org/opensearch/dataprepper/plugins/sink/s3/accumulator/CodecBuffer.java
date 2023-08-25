package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.sink.s3.codec.BufferedCodec;

import java.io.OutputStream;
import java.time.Duration;

public class CodecBuffer implements Buffer {
    private final Buffer innerBuffer;
    private final BufferedCodec bufferedCodec;

    public CodecBuffer(Buffer innerBuffer, BufferedCodec bufferedCodec) {
        this.innerBuffer = innerBuffer;
        this.bufferedCodec = bufferedCodec;
    }

    @Override
    public long getSize() {
        return bufferedCodec.getSize()
                .orElseGet(innerBuffer::getSize);
    }

    @Override
    public int getEventCount() {
        return innerBuffer.getEventCount();
    }

    @Override
    public Duration getDuration() {
        return innerBuffer.getDuration();
    }

    @Override
    public void flushToS3() {
        innerBuffer.flushToS3();
    }

    @Override
    public OutputStream getOutputStream() {
        return innerBuffer.getOutputStream();
    }

    @Override
    public void setEventCount(int eventCount) {
        innerBuffer.setEventCount(eventCount);
    }

    @Override
    public String getKey() {
        return innerBuffer.getKey();
    }
}
