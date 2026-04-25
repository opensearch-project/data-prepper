/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SemanticEnrichmentIndexManager {

    private static final Logger LOG = LoggerFactory.getLogger(SemanticEnrichmentIndexManager.class);

    private final AwsCredentialsSupplier awsCredentialsSupplier;

    public SemanticEnrichmentIndexManager(final AwsCredentialsSupplier awsCredentialsSupplier) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }

    public void maybeCreateIndex(final ConnectionConfiguration connectionConfiguration,
                                 final SemanticEnrichmentConfig semanticConfig,
                                 final String indexAlias) throws IOException {
        if (semanticConfig == null || semanticConfig.getFields() == null
                || semanticConfig.getFields().isEmpty()) {
            return;
        }

        if (!connectionConfiguration.isAwsSigv4()) {
            LOG.warn("Semantic enrichment is only supported with AWS OpenSearch. Skipping index creation.");
            return;
        }

        final SemanticEnrichmentIndexCreator indexCreator = new SemanticEnrichmentIndexCreator(
                awsCredentialsSupplier, connectionConfiguration, semanticConfig);
        indexCreator.createIndex(indexAlias, semanticConfig);
    }
}
