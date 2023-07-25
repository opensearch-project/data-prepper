package org.opensearch.dataprepper.exceptions;

public class BufferWriteException extends RuntimeException {
    public BufferWriteException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
