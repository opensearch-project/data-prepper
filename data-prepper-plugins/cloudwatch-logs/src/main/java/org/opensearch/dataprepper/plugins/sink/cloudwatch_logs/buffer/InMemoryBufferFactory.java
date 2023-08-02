/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer;

public class InMemoryBufferFactory implements BufferFactory{
    @Override
    public Buffer getBuffer() {
        return new InMemoryBuffer();
    }
}
