/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents the connection between two services.
 */
public class ServiceConnection {
    public static final String SERVICE_CONNECTION = "ServiceConnection";

    @JsonProperty("service")
    private final Service service;

    @JsonProperty("remoteService")
    private final Service remoteService;

    @JsonProperty("eventType")
    private final String eventType;
    
    @JsonProperty("timestamp")
    private final Instant timestamp;
    
    @JsonProperty("hashCode")
    private final String hashCodeString;

    public ServiceConnection(final Service service, final Service remoteService, final Instant timestamp) {
        this.service = service;
        this.remoteService = remoteService;
        this.eventType = SERVICE_CONNECTION;
        this.timestamp = timestamp;
        this.hashCodeString = String.valueOf(Objects.hash(service, remoteService, eventType));
    }

    public Service getService() {
        return service;
    }

    public Service getRemoteService() {
        return remoteService;
    }

    public String getEventType() {
        return eventType;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getHashCodeString() {
        return hashCodeString;
    }


    @Override
    public String toString() {
        return "ServiceConnection{" +
                "service=" + service +
                ", remoteService=" + remoteService +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                ", hashCodeString='" + hashCodeString + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ServiceConnection that = (ServiceConnection) o;
        return Objects.equals(service, that.service) && Objects.equals(remoteService, that.remoteService)
                && Objects.equals(eventType, that.eventType) && Objects.equals(timestamp, that.timestamp)
                && Objects.equals(hashCodeString, that.hashCodeString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(service, remoteService, eventType, timestamp, hashCodeString);
    }
}
