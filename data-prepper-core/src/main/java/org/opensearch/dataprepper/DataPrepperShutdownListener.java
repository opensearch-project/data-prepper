/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

@FunctionalInterface
public interface DataPrepperShutdownListener {
    void handleShutdown();
}
