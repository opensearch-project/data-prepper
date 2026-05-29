/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

public interface PullEngine {
    void initialize(String topicName, int partitionCount);

    void write(int partition, String key, byte[] document);

    void flush();

    void shutdown();
}
