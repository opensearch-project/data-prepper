/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.loghttp;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class LogHTTPSourceTest {
    // TODO: write test cases
    public static void main(String[] args) {
        LogHTTPSource logHTTPSource = new LogHTTPSource(new PluginSetting("log_http_source", new HashMap<>()));
        Buffer<Record<String>> blockingBuffer = new BlockingBuffer<Record<String>>(10, 8, "test-pipeline");
        logHTTPSource.start(blockingBuffer);
    }
}