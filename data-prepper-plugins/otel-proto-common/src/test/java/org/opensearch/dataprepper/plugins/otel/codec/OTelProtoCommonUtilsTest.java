/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;

import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

import java.time.Instant;
import java.util.UUID;

public class OTelProtoCommonUtilsTest {
    @Test
    public void test_convertUnixNanosToISO8601() {
        Instant curTime = Instant.now();
        assertThat(OTelProtoCommonUtils.convertUnixNanosToISO8601(curTime.getEpochSecond() * OTelProtoCommonUtils.NANO_MULTIPLIER + curTime.getNano()), equalTo(curTime.toString()));
        
    }

    @Test
    public void test_timeISO8601ToNanos() {
        Instant curTime = Instant.now();
        assertThat(curTime.getEpochSecond() * OTelProtoCommonUtils.NANO_MULTIPLIER + curTime.getNano(), equalTo(OTelProtoCommonUtils.timeISO8601ToNanos(curTime.toString())));
    }

    @Test
    public void test_convertByteStringToString() throws Exception {
        final String testString = UUID.randomUUID().toString();
        assertThat(ByteString.copyFrom(Hex.decodeHex(OTelProtoCommonUtils.convertByteStringToString(ByteString.copyFromUtf8(testString)))).toStringUtf8(), equalTo(testString));
    }
}
