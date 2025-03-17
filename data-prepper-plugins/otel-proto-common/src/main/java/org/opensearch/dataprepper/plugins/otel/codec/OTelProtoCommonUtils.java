/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import java.time.Instant;

public class OTelProtoCommonUtils {
    public static final long NANO_MULTIPLIER = 1_000 * 1_000 * 1_000;
    public static String convertUnixNanosToISO8601(final long unixNano) {
        return Instant.ofEpochSecond(0L, unixNano).toString();
    }

    public static long timeISO8601ToNanos(final String timeISO08601) {
        final Instant instant = Instant.parse(timeISO08601);
        return instant.getEpochSecond() * NANO_MULTIPLIER + instant.getNano();
    }

    public static String convertByteStringToString(ByteString bs) {
        return Hex.encodeHexString(bs.toByteArray());
    }
}

