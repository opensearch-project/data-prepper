/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.fs;

import org.apache.parquet.io.PositionOutputStream;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LocalFilePositionOutputStream extends PositionOutputStream {

    private final File file;
    private final RandomAccessFile fileStream;
    private boolean closed = false;

    LocalFilePositionOutputStream(final File file, final RandomAccessFile fileStream) {
        this.file = file;
        this.fileStream = fileStream;
    }

    public static LocalFilePositionOutputStream create(final File file) throws IOException {
        return new LocalFilePositionOutputStream(file, new RandomAccessFile(file, "rw"));
    }

    @Override
    public long getPos() throws IOException {
        if (this.closed) {
            return file.length();
        }
        return fileStream.getFilePointer();
    }

    @Override
    public void write(byte[] b) throws IOException {
        fileStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        fileStream.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        fileStream.write(b);
    }

    @Override
    public void close() throws IOException {
        if (!this.closed) {
            fileStream.close();
            this.closed = true;
        }
    }

    public boolean isClosed() {
        return this.closed;
    }

}
