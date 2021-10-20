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

package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;

import java.util.concurrent.TimeoutException;

@DataPrepperPlugin(name = "junit-test", pluginType = Source.class)
public class ConstructorLessComponent implements Source<Record<String>> {

    @Override
    public void start(Buffer<Record<String>> buffer) {
        try {
            buffer.write(new Record<>("Junit Testing"), 1_000);
        } catch (TimeoutException ex) {
            throw new RuntimeException("Timed out writing to buffer");
        }
    }

    @Override
    public void stop() {
        //no op
    }
}
