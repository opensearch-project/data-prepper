package org.opensearch.dataprepper.plugins.source;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import org.apache.http.ConnectionClosedException;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

class S3InputStream extends SeekableInputStream {

    static final List<Class<? extends Throwable>> RETRYABLE_EXCEPTIONS = List.of(
        ConnectionClosedException.class,
        EOFException.class,
        SocketException.class,
        SocketTimeoutException.class
    );

    private static final int COPY_BUFFER_SIZE = 8192;

    private static final Logger LOG = LoggerFactory.getLogger(S3InputStream.class);

    private static final int SKIP_SIZE = 1024 * 1024;

    private final S3Client s3Client;

    private final S3ObjectReference s3ObjectReference;

    private final HeadObjectResponse metadata;

    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;

    private final LongAdder bytesCounter;

    private final GetObjectRequest.Builder getObjectRequestBuilder;

    private InputStream stream;

    private final byte[] temp = new byte[COPY_BUFFER_SIZE];

    private long pos = 0;
    private long next = 0;

    private long mark = 0;

    private long markLimit = 0;

    private boolean closed = false;

    private RetryPolicy<byte[]> retryPolicyReturningByteArray;

    private RetryPolicy<Integer> retryPolicyReturningInteger;

    public S3InputStream(
        final S3Client s3Client,
        final S3ObjectReference s3ObjectReference,
        final HeadObjectResponse metadata,
        final S3ObjectPluginMetrics s3ObjectPluginMetrics,
        final Duration retryDelay,
        final int retries
    ) {
        this.s3Client = s3Client;
        this.s3ObjectReference = s3ObjectReference;
        this.metadata = metadata;
        this.s3ObjectPluginMetrics = s3ObjectPluginMetrics;
        this.bytesCounter = new LongAdder();

        this.getObjectRequestBuilder = GetObjectRequest.builder()
            .bucket(this.s3ObjectReference.getBucketName())
            .key(this.s3ObjectReference.getKey());

        this.retryPolicyReturningByteArray = RetryPolicy.<byte[]>builder()
            .handle(RETRYABLE_EXCEPTIONS)
            .withDelay(retryDelay)
            .withMaxRetries(retries)
            .build();

        this.retryPolicyReturningInteger = RetryPolicy.<Integer>builder()
            .handle(RETRYABLE_EXCEPTIONS)
            .withDelay(retryDelay)
            .withMaxRetries(retries)
            .build();
    }


    // Implement all InputStream methods first:

    /**
     * Returns bytes available to read.
     *
     * @throws IOException If the underlying stream throws IOException
     */
    @Override
    public int available() throws IOException {
        Preconditions.checkState(!closed, "Cannot read: already closed");
        positionStream();

        return stream.available();
    }

    /**
     * Close a stream.
     *
     * @throws IOException If the underlying stream throws IOException
     */
    @Override
    public void close() throws IOException {
        super.close();
        closed = true;
        closeStream();
        s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary().record(bytesCounter.doubleValue());
    }

    /**
     * Mark the current position of the input stream
     *
     * @param readlimit the maximum limit of bytes that can be read before
     * the mark position becomes invalid.
     */
    @Override
    public synchronized void mark(int readlimit) {
        mark = next;
        markLimit = mark + readlimit;
    }

    /**
     * Whether this stream supports mark or not.
     * @return Whether mark is supported or not.
     */
    @Override
    public synchronized boolean markSupported() {
        return true;
    }


    /**
     * Read a single byte from the stream
     * @return the number of bytes read
     * @throws IOException if data cannoy be read.
     */
    @Override
    public int read() throws IOException {
        Preconditions.checkState(!closed, "Cannot read: already closed");
        positionStream();

        final int byteRead = executeWithRetriesAndReturnInt(() -> stream.read());

        if (byteRead != -1) {
            pos += 1;
            next += 1;
            bytesCounter.increment();
        }

        return byteRead;
    }

    /**
     * Read data into the provided byte array
     * @param b   the buffer into which the data is read.
     * @return number of bytes read
     * @throws IOException if data cannot be read
     */
    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    /**
     * Read len bytes into the provided byte array starting at off
     * @param b     the buffer into which the data is read.
     * @param off   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param len   the maximum number of bytes to read.
     * @return number of bytes read
     * @throws IOException if data cannot be read
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Preconditions.checkState(!closed, "Cannot read: already closed");
        positionStream();

        final int bytesRead = executeWithRetriesAndReturnInt(() -> stream.read(b, off, len));

        if (bytesRead > 0) {
            pos += bytesRead;
            next += bytesRead;
            bytesCounter.add(bytesRead);
        }

        return bytesRead;
    }

    /**
     * Read all bytes from this input stream.
     * @return Array of bytes read
     * @throws IOException
     */
    @Override
    public byte[] readAllBytes() throws IOException {
        Preconditions.checkState(!closed, "Cannot read: already closed");
        positionStream();

        final byte[] bytesRead = executeWithRetriesAndReturnByteArray(() -> stream.readAllBytes());

        pos += bytesRead.length;
        next += bytesRead.length;
        bytesCounter.add(bytesRead.length);

        return bytesRead;
    }

    /**
     *
     * @param b the byte array into which the data is read
     * @param off the start offset in {@code b} at which the data is written
     * @param len the maximum number of bytes to read
     * @return number of bytes read
     * @throws IOException if underlying stream cannot be read from
     */
    @Override
    public int readNBytes(byte[] b, int off, int len) throws IOException {
        Preconditions.checkState(!closed, "Cannot read: already closed");
        positionStream();

        final int bytesRead = executeWithRetriesAndReturnInt(() -> stream.readNBytes(b, off, len));

        if (bytesRead > 0) {
            pos += bytesRead;
            next += bytesRead;
            bytesCounter.add(bytesRead);
        }

        return bytesRead;
    }

    /**
     * @param len the number of bytes to read
     * @return array of bytes read
     * @throws IOException if stream cannot be read from
     */
    @Override
    public byte[] readNBytes(int len) throws IOException {
        Preconditions.checkState(!closed, "Cannot read: already closed");
        positionStream();

        final byte[] bytesRead = executeWithRetriesAndReturnByteArray(() -> stream.readNBytes(len));

        pos += bytesRead.length;
        next += bytesRead.length;
        bytesCounter.add(bytesRead.length);

        return bytesRead;
    }

    /**
     * Reset the stream to the marked position
     * @throws IOException if the stream that was marked is no longer valid
     */
    @Override
    public synchronized void reset() throws IOException {
        if (next > markLimit) {
            throw new IOException("Cannot reset stream because mark limit exceeded");
        }

        next = mark;
    }

    /**
     * Skip n number of bytes in the stream.
     * @param n   the number of bytes to be skipped.
     * @return the number of bytes skipped.
     */
    @Override
    public long skip(long n) {
        if (next >= metadata.contentLength()) {
            return 0;
        }

        long toSkip = Math.min(n, metadata.contentLength() - next);

        next += toSkip;

        return toSkip;
    }

    // Override all SeekableInputStream methods

    /**
     * Get the offset into the stream
     * @return the offset into the stream
     */
    @Override
    public long getPos() {
        return next;
    }

    /**
     * Seek the specified offset into the input stream.
     * @param newPos the new position to seek to
     */
    @Override
    public void seek(long newPos) {
        Preconditions.checkState(!closed, "Cannot read: already closed");
        Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);

        // this allows a seek beyond the end of the stream but the next read will fail
        next = newPos;
    }

    // Implement all SeekableInputStream methods

    /**
     * Read a byte array of data, from position 0 to the end of the array.
     * <p>
     * This method is equivalent to {@code read(bytes, 0, bytes.length)}.
     * <p>
     * This method will block until len bytes are available to copy into the
     * array, or will throw {@link EOFException} if the stream ends before the
     * array is full.
     *
     * @param bytes a byte array to fill with data from the stream
     * @throws IOException If the underlying stream throws IOException
     * @throws EOFException If the stream has fewer bytes left than are needed to
     *                      fill the array, {@code bytes.length}
     */
    @Override
    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    /**
     * Read {@code len} bytes of data into an array, at position {@code start}.
     * <p>
     * This method will block until len bytes are available to copy into the
     * array, or will throw {@link EOFException} if the stream ends before the
     * array is full.
     *
     * @param bytes a byte array to fill with data from the stream
     * @param start the starting position in the byte array for data
     * @param len the length of bytes to read into the byte array
     * @throws IOException If the underlying stream throws IOException
     * @throws EOFException If the stream has fewer than {@code len} bytes left
     */
    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        Preconditions.checkState(!closed, "Cannot read: already closed");
        positionStream();

        final int bytesRead = executeWithRetriesAndReturnInt(() -> readFully(stream, bytes, start, len));

        if (bytesRead > 0) {
            this.pos += bytesRead;
            this.next += bytesRead;
            this.bytesCounter.add(bytesRead);
        }
    }

    /**
     * Read {@code buf.remaining()} bytes of data into a {@link ByteBuffer}.
     * <p>
     * This method will copy available bytes into the buffer, reading at most
     * {@code buf.remaining()} bytes. The number of bytes actually copied is
     * returned by the method, or -1 is returned to signal that the end of the
     * underlying stream has been reached.
     *
     * @param buf a byte buffer to fill with data from the stream
     * @return the number of bytes read or -1 if the stream ended
     * @throws IOException If the underlying stream throws IOException
     */
    @Override
    public int read(ByteBuffer buf) throws IOException {
        Preconditions.checkState(!closed, "Cannot read: already closed");
        positionStream();

        int bytesRead = 0;
        if (buf.hasArray()) {
            bytesRead = executeWithRetriesAndReturnInt(() -> readHeapBuffer(stream, buf));
        } else {
            bytesRead = executeWithRetriesAndReturnInt(() -> readDirectBuffer(stream, buf, temp));
        }

        if (bytesRead > 0) {
            this.pos += bytesRead;
            this.next += bytesRead;
            this.bytesCounter.add(bytesRead);
        }

        return bytesRead;
    }

    /**
     * Read {@code buf.remaining()} bytes of data into a {@link ByteBuffer}.
     * <p>
     * This method will block until {@code buf.remaining()} bytes are available
     * to copy into the buffer, or will throw {@link EOFException} if the stream
     * ends before the buffer is full.
     *
     * @param buf a byte buffer to fill with data from the stream
     * @throws IOException If the underlying stream throws IOException
     * @throws EOFException If the stream has fewer bytes left than are needed to
     *                      fill the buffer, {@code buf.remaining()}
     */
    @Override
    public void readFully(ByteBuffer buf) throws IOException {
        Preconditions.checkState(!closed, "Cannot read: already closed");
        positionStream();

        int bytesRead = 0;
        if (buf.hasArray()) {
            bytesRead = executeWithRetriesAndReturnInt(() -> readFullyHeapBuffer(stream, buf));
        } else {
            bytesRead = executeWithRetriesAndReturnInt(() -> readFullyDirectBuffer(stream, buf, temp));
        }

        if (bytesRead > 0) {
            this.pos += bytesRead;
            this.next += bytesRead;
            this.bytesCounter.add(bytesRead);
        }
    }

    /**
     * Position the stream for reading bytes starting at next offset
     * @throws IOException if stream cannot be set correctly
     */
    private void positionStream() throws IOException {

        if ((stream != null) && (next == pos)) {
            // already at specified position
            return;
        }

        if ((stream != null) && (next > pos)) {
            // seeking forwards
            long skip = next - pos;
            if (skip <= Math.max(stream.available(), SKIP_SIZE)) {
                // already buffered or seek is small enough
                LOG.debug("Read-through seek for {} to offset {}", s3ObjectReference, next);
                try {
                    ByteStreams.skipFully(stream, skip);
                    pos = next;
                    return;
                } catch (IOException ignored) {
                    // will retry by re-opening the stream
                }
            }
        }

        // close the stream and open at desired position
        LOG.debug("Seek with new stream for {} to offset {}", s3ObjectReference, next);
        pos = next;
        openStream();
    }

    /**
     * Open the stream to the S3 object
     * @throws IOException if the stream cannot be opened.
     */
    private void openStream() throws IOException {
        closeStream();

        if (pos >= metadata.contentLength()) {
            stream = InputStream.nullInputStream();
            return;
        }

        final GetObjectRequest request = this.getObjectRequestBuilder
                .range(String.format("bytes=%s-", pos))
                .build();

        try {
            stream = s3Client.getObject(request, ResponseTransformer.toInputStream());
        } catch (Exception ex) {
            LOG.error("Error reading from S3 object: s3ObjectReference={}", s3ObjectReference);
            if (ex instanceof S3Exception) {
                recordS3Exception((S3Exception) ex);
            }

            throw new IOException(ex.getMessage());
        }
    }

    /**
     * Close the input stream from the S3 object
     * @throws IOException if the stream cannot be closed.
     */
    private void closeStream() throws IOException {
        if (stream != null) {
            // if we aren't at the end of the stream, and the stream is abortable, then
            // call abort() so we don't read the remaining data with the Apache HTTP client
            abortStream();
            try {
                stream.close();
            } catch (IOException e) {
                // the Apache HTTP client will throw a ConnectionClosedException
                // when closing an aborted stream, which is expected
                if (!e.getClass().getSimpleName().equals("ConnectionClosedException")) {
                    throw e;
                }
            }
            stream = null;
        }
    }

    /**
     * Abort the stream to the S3 object.
     */
    private void abortStream() {
        try {
            if (stream instanceof Abortable) {
                ((Abortable) stream).abort();
            }
        } catch (Exception e) {
            LOG.warn("An error occurred while aborting the stream", e);
        }
    }

    /**
     * Read the input stream into the byte buffer with the assumption that the byte buffer is backed by some bytes.
     * @param f input stream
     * @param buf byte buffer wrapper
     * @return bytes read into the buffer
     * @throws IOException if bytes cannot be read from input stream into the byte buffer
     */
    // Visible for testing
    static int readHeapBuffer(InputStream f, ByteBuffer buf) throws IOException {
        int bytesRead = f.read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        if (bytesRead < 0) {
            // if this resulted in EOF, don't update position
            return bytesRead;
        } else {
            buf.position(buf.position() + bytesRead);

            return bytesRead;
        }
    }

    /**
     * Helper method to read bytes from an input stream into a byte array
     * @param f input stream
     * @param bytes byte array
     * @param start offset into byte array to start reading to
     * @param len number of bytes to read into the byte array
     * @return number of bytes read into buffer
     * @throws IOException if input stream cannot be read
     */
    static int readFully(InputStream f, byte[] bytes, int start, int len) throws IOException {
        int totalBytesRead = 0;
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
            totalBytesRead += bytesRead;
        }
        return totalBytesRead;
    }

    /**
     * Read fully into the bytes buffer assuming that the byte buffer is backed by a byte array
     * @param f input stream
     * @param buf byte buffer
     * @return number of bytes read into buffer
     * @throws IOException if bytes cannot be read into the byte buffer
     */
    // Visible for testing
    static int readFullyHeapBuffer(InputStream f, ByteBuffer buf) throws IOException {
        int bytesRead = readFully(f, buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        buf.position(buf.limit());
        return bytesRead;
    }

    /**
     * Read into a direct buffer with the assumption that the byte buffer has no backing byte array
     * @param f input stream
     * @param buf byte buffer
     * @param temp byte array to use as a buffer
     * @return the number of bytes read
     * @throws IOException if the bytes cannot be read from the input stream
     */
    // Visible for testing
    static int readDirectBuffer(InputStream f, ByteBuffer buf, byte[] temp) throws IOException {
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

    /**
     * Read into from the input stream into the byte buffer using the provided byte array as a buffer
     * @param f input sream to read from
     * @param buf byte buffer to read data into
     * @param temp The byte array to use as a buffer for reading.
     * @return number of bytes read into buffer
     * @throws IOException if the bytes cannot be read
     */
    // Visible for testing
    static int readFullyDirectBuffer(InputStream f, ByteBuffer buf, byte[] temp) throws IOException {
        int totalBytesRead = 0;
        int nextReadLength = Math.min(buf.remaining(), temp.length);
        int bytesRead = 0;

        while (nextReadLength > 0 && (bytesRead = f.read(temp, 0, nextReadLength)) >= 0) {
            buf.put(temp, 0, bytesRead);
            nextReadLength = Math.min(buf.remaining(), temp.length);
            totalBytesRead += bytesRead;
        }

        if (bytesRead < 0 && buf.remaining() > 0) {
            throw new EOFException(
                    "Reached the end of stream with " + buf.remaining() + " bytes left to read");
        }

        return totalBytesRead;
    }

    private void recordS3Exception(final S3Exception ex) {
        if (ex.statusCode() == HttpStatusCode.NOT_FOUND) {
            s3ObjectPluginMetrics.getS3ObjectsFailedNotFoundCounter().increment();
        } else if (ex.statusCode() == HttpStatusCode.FORBIDDEN) {
            s3ObjectPluginMetrics.getS3ObjectsFailedAccessDeniedCounter().increment();
        }
    }

    private int executeWithRetriesAndReturnInt(CheckedSupplier<Integer> supplier) throws IOException {
        return executeWithRetries(retryPolicyReturningInteger, supplier);
    }

    private byte[] executeWithRetriesAndReturnByteArray(CheckedSupplier<byte[]> supplier) throws IOException {
        return executeWithRetries(retryPolicyReturningByteArray, supplier);
    }


    private <T> T executeWithRetries(RetryPolicy<T> retryPolicy, CheckedSupplier<T> supplier) throws IOException {
        try {
            return Failsafe.with(retryPolicy).get(() -> {
                try {
                    return supplier.get();
                } catch (ConnectionClosedException | EOFException | SocketException | SocketTimeoutException e) {
                    LOG.warn("Resetting stream due to underlying socket exception", e);
                    openStream();
                    throw e;
                }
            });
        } catch (FailsafeException e) {
            LOG.error("Failed to read with Retries", e);
            throw new IOException(e.getCause());
        }

    }

}
