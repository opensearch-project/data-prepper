/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.opensearch.indices.CreateIndexRequest;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

interface IsmPolicyManagementStrategy {

    Optional<String> checkAndCreatePolicy(final String indexAlias) throws IOException;

    List<String> getIndexPatterns(final String indexAlias);

    boolean checkIfIndexExistsOnServer(final String indexAlias) throws IOException;

    CreateIndexRequest getCreateIndexRequest(final String indexAlias);
}
