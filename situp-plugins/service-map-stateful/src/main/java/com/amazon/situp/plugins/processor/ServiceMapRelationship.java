package com.amazon.situp.plugins.processor;

import java.util.Objects;

public class ServiceMapRelationship {
    private String serviceName;
    private String spanKind;
    private String destination;
    private String target;

    public ServiceMapRelationship() {
    }

    public ServiceMapRelationship(String serviceName, String spanKind, String destination, String target) {
        this.serviceName = serviceName;
        this.spanKind = spanKind;
        this.destination = destination;
        this.target = target;
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
        this.destination = destination;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
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
