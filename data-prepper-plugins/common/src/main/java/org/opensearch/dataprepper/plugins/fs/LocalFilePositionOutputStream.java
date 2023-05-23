/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.fs;

import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.RandomAccessFile;

public class LocalFilePositionOutputStream extends PositionOutputStream {

    private final RandomAccessFile stream;
    private boolean isClosed = false;

    public LocalFilePositionOutputStream(RandomAccessFile stream) {
        this.stream = stream;
    }

    @Override
    public long getPos() throws IOException {
        if (isClosed) {
            return stream.length();
        }
        return stream.getFilePointer();
    }

    @Override
    public void write(byte[] b) throws IOException {
        stream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        stream.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        stream.write(b);
    }

    @Override
    public void close() throws IOException {
        stream.close();
        this.isClosed = true;
    }

    public boolean isClosed() {
        return this.isClosed;
    }

}
