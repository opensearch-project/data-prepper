/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

interface DataPrepperShutdownListener {
    void handleShutdown();
}
