/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.model;

public class ServiceInfo {

    private String distribution;
    private Integer version;

    public String getDistribution() {
        return distribution;
    }

    public Integer getVersion() {
        return version;
    }

    public void setDistribution(String distribution) {
        this.distribution = distribution;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }
}
