/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class LocalInputFile implements InputFile {
    private final File file;

    LocalInputFile(File file) {
        this.file = file;
    }

    @Override
    public long getLength() {
        return file.length();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new FileSeekableInputStream(file);
    }

    private static class FileSeekableInputStream extends SeekableInputStream {
        private final int COPY_BUFFER_SIZE = 8192;
        private final byte[] temp = new byte[COPY_BUFFER_SIZE];

        private final RandomAccessFile randomAccessFile;

        FileSeekableInputStream(File file) throws FileNotFoundException {
            randomAccessFile = new RandomAccessFile(file, "r");
        }

        @Override
        public long getPos() throws IOException {
            return randomAccessFile.getFilePointer();
        }

        @Override
        public void seek(long newPos) throws IOException {
            randomAccessFile.seek(newPos);
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            randomAccessFile.readFully(bytes);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            randomAccessFile.read(bytes, start, len);
        }

        @Override
        public int read() throws IOException {
            return randomAccessFile.read();
        }

        /*
        The following has been copied from DelegatingSeekableInputStream.
        It was modified by replacing InputStream with RandomAccessFile.
         */

        @Override
        public int read(ByteBuffer buf) throws IOException {
            if (buf.hasArray()) {
                return readHeapBuffer(randomAccessFile, buf);
            } else {
                return readDirectBuffer(randomAccessFile, buf, temp);
            }
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            if (buf.hasArray()) {
                readFullyHeapBuffer(randomAccessFile, buf);
            } else {
                readFullyDirectBuffer(randomAccessFile, buf, temp);
            }
        }

        static void readFully(RandomAccessFile f, byte[] bytes, int start, int len) throws IOException {
            int offset = start;
            int remaining = len;
            while (remaining > 0) {
                int bytesRead = f.read(bytes, offset, remaining);
                if (bytesRead < 0) {
                    throw new EOFException(
                            "Reached the end of stream with " + remaining + " bytes left to read");
                }

                remaining -= bytesRead;
                offset += bytesRead;
            }
        }

        static int readHeapBuffer(RandomAccessFile f, ByteBuffer buf) throws IOException {
            int bytesRead = f.read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
            if (bytesRead < 0) {
                // if this resulted in EOF, don't update position
                return bytesRead;
            } else {
                buf.position(buf.position() + bytesRead);
                return bytesRead;
            }
        }

        static void readFullyHeapBuffer(RandomAccessFile f, ByteBuffer buf) throws IOException {
            readFully(f, buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
            buf.position(buf.limit());
        }

        static int readDirectBuffer(RandomAccessFile f, ByteBuffer buf, byte[] temp) throws IOException {
            // copy all the bytes that return immediately, stopping at the first
            // read that doesn't return a full buffer.
            int nextReadLength = Math.min(buf.remaining(), temp.length);
            int totalBytesRead = 0;
            int bytesRead;

            while ((bytesRead = f.read(temp, 0, nextReadLength)) == temp.length) {
                buf.put(temp);
                totalBytesRead += bytesRead;
                nextReadLength = Math.min(buf.remaining(), temp.length);
            }

            if (bytesRead < 0) {
                // return -1 if nothing was read
                return totalBytesRead == 0 ? -1 : totalBytesRead;
            } else {
                // copy the last partial buffer
                buf.put(temp, 0, bytesRead);
                totalBytesRead += bytesRead;
                return totalBytesRead;
            }
        }

        static void readFullyDirectBuffer(RandomAccessFile f, ByteBuffer buf, byte[] temp) throws IOException {
            int nextReadLength = Math.min(buf.remaining(), temp.length);
            int bytesRead = 0;

            while (nextReadLength > 0 && (bytesRead = f.read(temp, 0, nextReadLength)) >= 0) {
                buf.put(temp, 0, bytesRead);
                nextReadLength = Math.min(buf.remaining(), temp.length);
            }

            if (bytesRead < 0 && buf.remaining() > 0) {
                throw new EOFException(
                        "Reached the end of stream with " + buf.remaining() + " bytes left to read");
            }
        }
    }
}
