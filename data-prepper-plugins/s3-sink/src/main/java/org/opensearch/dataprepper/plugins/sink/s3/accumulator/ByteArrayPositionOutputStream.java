package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.apache.parquet.io.DelegatingPositionOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ByteArrayPositionOutputStream extends DelegatingPositionOutputStream {
    private final ByteArrayOutputStream innerOutputStream;

    public ByteArrayPositionOutputStream(ByteArrayOutputStream innerOutputStream) {
        super(innerOutputStream);
        this.innerOutputStream = innerOutputStream;
    }

    @Override
    public long getPos() throws IOException {
        return innerOutputStream.size();
    }
}
