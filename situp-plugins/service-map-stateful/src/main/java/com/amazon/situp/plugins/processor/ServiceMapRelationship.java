package com.amazon.situp.plugins.processor;

import java.util.Objects;


public class ServiceMapRelationship {
    /**
     * Service name for the relationship. This corresponds to the source of the relationship
     */
    private String serviceName;

    /**
     * Span kind for the relationship. This corresponds to the source span from the relationship
     */
    private String spanKind;

    /**
     * Destination for the relationship. Meant to be correlated to a "target" field on a separate relationship
     */
    private String destination;

    /**
     * Target for the relationship. Relates a "destination" from another relationship to a service name.
     */
    private String target;

    public ServiceMapRelationship() {
    }

    private ServiceMapRelationship(String serviceName, String spanKind, String destination, String target) {
        this.serviceName = serviceName;
        this.spanKind = spanKind;
        this.destination = destination;
        this.target = target;
    }

    /**
     * Create a destination relationship
     * @return Relationship with the fields set, and target set to null
     */
    public static ServiceMapRelationship newDestinationRelationship (
            final String serviceName,
            final String spanKind,
            final String destination) {
        return new ServiceMapRelationship(serviceName, spanKind, destination, null);
    }

    /**
     * Create a target relationship
     * @return Relationship with the fields set, and destination set to null
     */
    public static ServiceMapRelationship newTargetRelationship (
            final String serviceName,
            final String spanKind,
            final String target) {
        return new ServiceMapRelationship(serviceName, spanKind, null, target);
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getSpanKind() {
        return spanKind;
    }

    public void setSpanKind(String spanKind) {
        this.spanKind = spanKind;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        if(destination != null && target != null) {
            throw new RuntimeException("Cannot set both target and destination.");
        }
        this.destination = destination;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        if(target != null && destination != null) {
            throw new RuntimeException("Cannot set both target and destination.");
        }
        this.target = target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceMapRelationship that = (ServiceMapRelationship) o;
        return Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(spanKind, that.spanKind) &&
                Objects.equals(destination, that.destination) &&
                Objects.equals(target, that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, spanKind, destination, target);
    }
}
