/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.source.prometheus;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import java.time.Duration;
import java.util.List;

public class PrometheusScrapeConfig {

    @JsonProperty("targets")
    @NotEmpty
    @Valid
    private List<ScrapeTargetConfig> targets;

    @JsonProperty("scrape_interval")
    private Duration scrapeInterval = Duration.ofSeconds(15);

    @JsonProperty("scrape_timeout")
    private Duration scrapeTimeout = Duration.ofSeconds(10);

    @JsonProperty("flatten_labels")
    private boolean flattenLabels = false;

    @JsonProperty("insecure")
    private boolean insecure = false;

    @JsonProperty("authentication")
    private ScrapeAuthenticationConfig authentication;

    @JsonProperty("ssl_certificate_file")
    private String sslCertificateFile;

    public List<ScrapeTargetConfig> getTargets() {
        return targets;
    }

    public Duration getScrapeInterval() {
        return scrapeInterval;
    }

    public Duration getScrapeTimeout() {
        return scrapeTimeout;
    }

    public boolean isFlattenLabels() {
        return flattenLabels;
    }

    public boolean isInsecure() {
        return insecure;
    }

    public ScrapeAuthenticationConfig getAuthentication() {
        return authentication;
    }

    public String getSslCertificateFile() {
        return sslCertificateFile;
    }
}