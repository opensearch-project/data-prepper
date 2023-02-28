/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.stream;

import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * An {@code OutputStream} which packages data written to it into discrete {@link StreamPart}s which can be obtained
 * in a separate thread via iteration and uploaded to S3.
 * <p>
 * A single {@code MultiPartOutputStream} is allocated a range of part numbers it can assign to the {@code StreamPart}s
 * it produces, which is determined at construction.
 * <p>
 * It's essential to call
 * {@link MultiPartOutputStream#close()} when finished so that it can create the final {@code StreamPart} and consumers
 * can finish.
 * <p>
 * Writing to the stream may lead to trying to place a completed part on a queue,
 * which will block if the queue is full and may lead to an {@code InterruptedException}.
 */
public class MultiPartOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(MultiPartOutputStream.class);
    
    public static final int KB = 1024;
	/** Megabytes */
	public static final int MB = 1024 * KB;

    private ConvertibleOutputStream currentStream;

    public static final int S3_MIN_PART_SIZE = 5 * MB;
    private static final int STREAM_EXTRA_ROOM = MB;

    private BlockingQueue<StreamPart> queue;

    private final int partNumberStart;
    private final int partNumberEnd;
    private final int partSize;
    private int currentPartNumber;

    /**
     * Creates a new stream that will produce parts of the given size with part numbers in the given range.
     *
     * @param partNumberStart the part number of the first part the stream will produce. Minimum 1.
     * @param partNumberEnd   1 more than the last part number that the parts are allowed to have. Maximum 10 001.
     * @param partSize        the minimum size in bytes of parts to be produced.
     * @param queue           where stream parts are put on production.
     */
    MultiPartOutputStream(int partNumberStart, int partNumberEnd, int partSize, BlockingQueue<StreamPart> queue) {
        if (partNumberStart < 1) {
            throw new IndexOutOfBoundsException("The lowest allowed part number is 1. The value given was " + partNumberStart);
        }
        if (partNumberEnd > 10001) {
            throw new IndexOutOfBoundsException(
                    "The highest allowed part number is 10 000, so partNumberEnd must be at most 10 001. The value given was " + partNumberEnd);
        }
        if (partNumberEnd <= partNumberStart) {
            throw new IndexOutOfBoundsException(
                    String.format("The part number end (%d) must be greater than the part number start (%d).", partNumberEnd, partNumberStart));
        }
        if (partSize < S3_MIN_PART_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "The given part size (%d) is less than 5 MB.", partSize));
        }

        this.partNumberStart = partNumberStart;
        this.partNumberEnd = partNumberEnd;
        this.queue = queue;
        this.partSize = partSize;

        log.debug("Creating {}", this);

        currentPartNumber = partNumberStart;
        currentStream = new ConvertibleOutputStream(getStreamAllocatedSize());
    }

    /**
     * Returns the initial capacity in bytes of the {@code ByteArrayOutputStream} that a part uses.
     */
    private int getStreamAllocatedSize() {
        /*
        This consists of the size that the user asks for, the extra 5 MB to avoid small parts (see the comment in
        checkSize()), and some extra space to make resizing and copying unlikely.
         */
        return partSize + S3_MIN_PART_SIZE + STREAM_EXTRA_ROOM;
    }

    /**
     * Checks if the stream currently contains enough data to create a new part.
     */
    private void checkSize() {
        /*
        This class avoids producing parts < 5 MB if possible by only producing a part when it has an extra 5 MB to spare
        for the next part. For example, suppose the following. A stream is producing parts of 10 MB. Someone writes
        10 MB and then calls this method, and then writes only 3 MB more before closing. If the initial 10 MB were
        immediately packaged into a StreamPart and a new ConvertibleOutputStream was started on for the rest, it would
        end up producing a part with just 3 MB. So instead the class waits until it contains 15 MB of data and then it
        splits the stream into two: one with 10 MB that gets produced as a part, and one with 5 MB that it continues with.
        In this way users of the class are less likely to encounter parts < 5 MB which cause trouble: see the caveat
        on order in the StreamTransferManager and the way it handles these small parts, referred to as 'leftover'.
        Such parts are only produced when the user closes a stream that never had more than 5 MB written to it.
         */
        if (currentStream == null) {
            throw new IllegalStateException("The stream is closed and cannot be written to.");
        }
        if (currentStream.size() > partSize + S3_MIN_PART_SIZE) {
            ConvertibleOutputStream newStream = currentStream.split(
                    currentStream.size() - S3_MIN_PART_SIZE,
                    getStreamAllocatedSize());
            putCurrentStream();
            currentStream = newStream;
        }
    }

    private void putCurrentStream() {
        if (currentStream.size() == 0) {
            return;
        }
        if (currentPartNumber >= partNumberEnd) {
            throw new IndexOutOfBoundsException(
                    String.format("This stream was allocated the part numbers from %d (inclusive) to %d (exclusive)" +
                                    "and it has gone beyond the end.",
                            partNumberStart, partNumberEnd));
        }
        StreamPart streamPart = new StreamPart(currentStream, currentPartNumber++);
        log.debug("Putting {} on queue", streamPart);
        try {
            queue.put(streamPart);
        } catch (InterruptedException e) {
            throw Utils.runtimeInterruptedException(e);
        }
    }

    @Override
    public void write(int b) {
        currentStream.write(b);
        checkSize();
    }

    @Override
    public void write(byte b[], int off, int len) {
        currentStream.write(b, off, len);
        checkSize();
    }

    @Override
    public void write(byte b[]) {
        write(b, 0, b.length);
        checkSize();
    }

    /**
     * Packages any remaining data into a {@link StreamPart} and signals to the {@code StreamTransferManager} that there are no more parts
     * afterwards. You cannot write to the stream after it has been closed.
     */
    @Override
    public void close() {
        //log.info("Called close() on {}", this);
        if (currentStream == null) {
            log.warn("{} is already closed", this);
            return;
        }
        try {
            putCurrentStream();
            log.debug("Placing poison pill on queue for {}", this);
            queue.put(StreamPart.POISON);
        } catch (InterruptedException e) {
            log.error("Interrupted while closing {}", this);
            throw Utils.runtimeInterruptedException(e);
        }
        currentStream = null;
    }

    @Override
    public String toString() {
        return String.format("[MultipartOutputStream for parts %d - %d]", partNumberStart, partNumberEnd - 1);
    }
}
