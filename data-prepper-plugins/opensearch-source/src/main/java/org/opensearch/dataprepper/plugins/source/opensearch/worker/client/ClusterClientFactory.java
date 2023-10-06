/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import org.opensearch.dataprepper.plugins.source.opensearch.ClientRefresher;

public interface ClusterClientFactory<Client> {
    ClientRefresher<Client> getClientRefresher();
}
