/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.atlassian;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;

import java.util.List;

@Getter
public abstract class AtlassianSourceConfig implements CrawlerSourceConfig {

    private static final int DEFAULT_BATCH_SIZE = 50;

    /**
     * Jira account url
     */
    @JsonProperty("hosts")
    protected List<String> hosts;

    /**
     * Authentication Config to Access Jira
     */
    @JsonProperty("authentication")
    @Valid
    protected AuthenticationConfig authenticationConfig;

    /**
     * Batch size for fetching tickets
     */
    @JsonProperty("batch_size")
    protected int batchSize = DEFAULT_BATCH_SIZE;


    /**
     * Boolean property indicating end to end acknowledgments state
     */
    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    public String getAccountUrl() {
        return this.getHosts().get(0);
    }

    public String getAuthType() {
        return this.getAuthenticationConfig().getAuthType();
    }

    public abstract String getOauth2UrlContext();
}
