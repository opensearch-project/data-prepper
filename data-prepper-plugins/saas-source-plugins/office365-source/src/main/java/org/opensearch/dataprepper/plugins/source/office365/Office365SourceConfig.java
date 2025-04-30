/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.office365;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.office365.auth.AuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;

/**
 * Configuration class for Office 365 source plugin.
 */
@Getter
public class Office365SourceConfig implements CrawlerSourceConfig {
    /**
     * Default number of records to retrieve in a single batch.
     */
    private static final int DEFAULT_BATCH_SIZE = 50;

    /**
     * The Office 365 tenant ID that uniquely identifies the Microsoft Entra organization.
     */
    @NotNull
    @JsonProperty("tenant_id")
    private String tenantId;

    /**
     * Authentication configuration for accessing Office 365 APIs.
     * Contains OAuth2 credentials including client ID and client secret for now
     */
    @NotNull
    @JsonProperty("authentication")
    @Valid
    private AuthenticationConfiguration authenticationConfiguration;

    @JsonProperty("batch_size")
    private int batchSize = DEFAULT_BATCH_SIZE;

    /**
     * Flag to enable/disable acknowledgments for processed records.
     * When enabled, ensures records are processed at least once.
     */
    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    @Override
    public int getNumberOfWorkers() {
        return DEFAULT_NUMBER_OF_WORKERS;
    }

    @Override
    public boolean isAcknowledgments() {
        return acknowledgments;
    }
}
