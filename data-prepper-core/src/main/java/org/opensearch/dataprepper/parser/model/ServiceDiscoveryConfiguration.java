/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.dataprepper.peerforwarder.discovery.DiscoveryMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ServiceDiscoveryConfiguration {

    private DiscoveryMode discoveryMode = DiscoveryMode.LOCAL_NODE;
    private String awsCloudMapNamespaceName;
    private String awsCloudMapServiceName;
    private String awsRegion;
    private Map<String, String> awsCloudMapQueryParameters = Collections.emptyMap();
    private String domainName;
    private List<String> staticEndpoints = new ArrayList<>();

    public ServiceDiscoveryConfiguration() {

    }

    @JsonCreator
    public ServiceDiscoveryConfiguration(@JsonProperty("discovery_mode") final String discoveryMode,
                                         @JsonProperty("aws_cloud_map_namespace_name") final String awsCloudMapNamespaceName,
                                         @JsonProperty("aws_cloud_map_service_name") final String awsCloudMapServiceName,
                                         @JsonProperty("aws_region") final String awsRegion,
                                         @JsonProperty("aws_cloud_map_query_parameters") final Map<String, String> awsCloudMapQueryParameters,
                                         @JsonProperty("domain_name") final String domainName,
                                         @JsonProperty("static_endpoints") final List<String> staticEndpoints) {
        setDiscoveryMode(discoveryMode);
        setAwsCloudMapNamespaceName(awsCloudMapNamespaceName);
        setAwsCloudMapServiceName(awsCloudMapServiceName);
        setAwsRegion(awsRegion);
        setAwsCloudMapQueryParameters(awsCloudMapQueryParameters);
        setDomainName(domainName);
        setStaticEndpoints(staticEndpoints);

    }

    private void setDiscoveryMode(final String discoveryMode) {
        if (discoveryMode != null) {
            this.discoveryMode = DiscoveryMode.valueOf(discoveryMode.toUpperCase());
        }
    }

    private void setAwsCloudMapNamespaceName(final String awsCloudMapNamespaceName) {
        if (discoveryMode.equals(DiscoveryMode.AWS_CLOUD_MAP)) {
            if (awsCloudMapNamespaceName != null) {
                this.awsCloudMapNamespaceName = awsCloudMapNamespaceName;
            }
            else {
                throw new IllegalArgumentException("Cloud Map namespace cannot be null if discover mode is AWS Cloud Map.");
            }
        }
    }

    private void setAwsCloudMapServiceName(final String awsCloudMapServiceName) {
        if (discoveryMode.equals(DiscoveryMode.AWS_CLOUD_MAP)) {
            if (awsCloudMapServiceName != null) {
                this.awsCloudMapServiceName = awsCloudMapServiceName;
            }
            else {
                throw new IllegalArgumentException("Cloud Map service name cannot be null if discover mode is AWS Cloud Map.");
            }
        }
    }

    private void setAwsRegion(final String awsRegion) {
        if (discoveryMode.equals(DiscoveryMode.AWS_CLOUD_MAP)) {
            if (StringUtils.isNotEmpty(awsRegion)) {
                this.awsRegion = awsRegion;
            }
            else {
                throw new IllegalArgumentException("AWS region cannot be null if discover mode is AWS Cloud Map.");
            }
        }
    }

    private void setAwsCloudMapQueryParameters(Map<String, String> awsCloudMapQueryParameters) {
        if (awsCloudMapQueryParameters != null) {
            this.awsCloudMapQueryParameters = awsCloudMapQueryParameters;
        }
    }

    private void setDomainName(final String domainName) {
        if (discoveryMode.equals(DiscoveryMode.DNS)) {
            if (domainName != null) {
                this.domainName = domainName;
            }
            else {
                throw new IllegalArgumentException("Domain name cannot be null if discover mode is DNS.");
            }
        }
    }

    private void setStaticEndpoints(final List<String> staticEndpoints) {
        if (staticEndpoints != null) {
            this.staticEndpoints = staticEndpoints;
        }
    }

    public DiscoveryMode getDiscoveryMode() {
        return discoveryMode;
    }

    public String getAwsCloudMapNamespaceName() {
        return awsCloudMapNamespaceName;
    }

    public String getAwsCloudMapServiceName() {
        return awsCloudMapServiceName;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public Map<String, String> getAwsCloudMapQueryParameters() {
        return awsCloudMapQueryParameters;
    }

    public String getDomainName() {
        return domainName;
    }

    public List<String> getStaticEndpoints() {
        return staticEndpoints;
    }
}
