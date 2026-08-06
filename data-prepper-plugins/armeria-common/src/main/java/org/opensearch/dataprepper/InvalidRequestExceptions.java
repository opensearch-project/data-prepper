/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Classifies request-level failures that represent client-side data problems (as opposed to server
 * errors) so sources can respond with an informative 4xx instead of an opaque 500.
 *
 * <p>Detection walks the exception cause chain and keys off the stable, public
 * {@link InvalidProtocolBufferException} type rather than an Armeria-internal error message, so it does
 * not silently regress when the underlying framework rewords its messages.
 */
public final class InvalidRequestExceptions {

    /**
     * Marker used by Armeria when a compressed request frame cannot be decoded. Decompression failures do
     * not wrap an {@link InvalidProtocolBufferException}, so this remains a message-based fallback.
     */
    static final String COMPRESSED_FRAME_MARKER = "Can't decode compressed frame";

    public static final String INVALID_PROTOBUF_MESSAGE =
            "Invalid protobuf byte sequence: the request payload could not be parsed as valid protobuf. "
                    + "This is a problem with the client-side request or a misconfiguration of the pipeline source. Protobuf requires every string field to "
                    + "contain valid UTF-8. The request likely has a field such as the InstrumentationScope name or version, a resource/scope "
                    + "attribute key, or an attribute value that contains non-UTF-8 bytes. Check the instrumentation "
                    + "libraries producing this telemetry and correct the source emitting the malformed data.";

    public static final String COMPRESSED_FRAME_MESSAGE =
            "Unable to decode the compressed request frame. This is a client-side data issue, not a server error. "
                    + "Verify that the payload compression (for example, gzip) matches the compression configured on the "
                    + "pipeline source and that the payload is not truncated or corrupted.";

    private static final int MAX_CAUSE_DEPTH = 20;

    private InvalidRequestExceptions() {
    }

    /**
     * @return {@code true} if the throwable, or any cause within it, is a protobuf parse failure such as
     *         invalid UTF-8 in a string field.
     */
    public static boolean isInvalidProtobuf(final Throwable throwable) {
        Throwable current = throwable;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (current instanceof InvalidProtocolBufferException) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * @return {@code true} if the throwable, or any cause within it, is a request-frame decompression failure.
     */
    public static boolean isUndecodableCompressedFrame(final Throwable throwable) {
        Throwable current = throwable;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            final String message = current.getMessage();
            if (message != null && message.contains(COMPRESSED_FRAME_MARKER)) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return false;
    }
}
