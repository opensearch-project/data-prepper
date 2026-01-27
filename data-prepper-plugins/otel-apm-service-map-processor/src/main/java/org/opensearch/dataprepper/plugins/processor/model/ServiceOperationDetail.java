/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Represents the details about a service operation.
 */
public class ServiceOperationDetail {

    public static final String SERVICE_OPERATION_DETAIL = "ServiceOperationDetail";

    @JsonProperty("service")
    private final Service service;

    @JsonProperty("operation")
    private final Operation operations;

    @JsonProperty("eventType")
    private final String eventType;

    @JsonProperty("timestamp")
    private final String timestamp;

    @JsonProperty("hashCode")
    private final String hashCodeString;

    public ServiceOperationDetail(Service service, Operation operations, Instant timestamp) {
        this.service = service;
        this.operations = operations;
        this.eventType = SERVICE_OPERATION_DETAIL;
        this.timestamp = DateTimeFormatter.ISO_INSTANT.format(timestamp);
        this.hashCodeString = String.valueOf(Objects.hash(service, operations, eventType));
    }

    public Service getService() {
        return service;
    }

    public Operation getOperations() {
        return operations;
    }

    public String getEventType() {
        return eventType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getHashCodeString() {
        return hashCodeString;
    }

    @Override
    public String toString() {
        return "ServiceOperationDetail{" +
                "Service=" + service +
                ", operations=" + operations +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                ", hashCodeString='" + hashCodeString + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ServiceOperationDetail that = (ServiceOperationDetail) o;
        return Objects.equals(service, that.service) && Objects.equals(operations, that.operations)
                && Objects.equals(eventType, that.eventType) && Objects.equals(timestamp, that.timestamp)
                && Objects.equals(hashCodeString, that.hashCodeString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(service, operations, eventType, timestamp, hashCodeString);
    }
}
