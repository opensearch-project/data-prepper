/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.buffer;

/**
 * BufferFactory will act as a means for decoupling the rest of
 * the code from the type of buffer being used.
 */
public interface BufferFactory {
    Buffer getBuffer();
}