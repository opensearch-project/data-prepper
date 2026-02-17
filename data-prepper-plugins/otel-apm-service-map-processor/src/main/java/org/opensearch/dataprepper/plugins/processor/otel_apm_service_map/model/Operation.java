/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Operation {

    @JsonProperty("name")
    private final String name;

    @JsonProperty("remoteService")
    private final Service remoteService;

    @JsonProperty("remoteOperationName")
    private final String remoteOperationName;

    public Operation(String name, Service remoteService, String remoteOperationName) {
        this.name = name;
        this.remoteService = remoteService;
        this.remoteOperationName = remoteOperationName;
    }

    public String getName() {
        return name;
    }

    public Service getRemoteService() {
        return remoteService;
    }

    public String getRemoteOperationName() {
        return remoteOperationName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Operation operation = (Operation) o;
        return Objects.equals(name, operation.name) && Objects.equals(remoteService, operation.remoteService) && Objects.equals(remoteOperationName, operation.remoteOperationName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, remoteService, remoteOperationName);
    }

    @Override
    public String toString() {
        return "Operation{" +
                "name='" + name + '\'' +
                ", remoteService=" + remoteService +
                ", remoteOperationName='" + remoteOperationName + '\'' +
                '}';
    }
}
