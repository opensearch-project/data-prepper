package com.amazon.situp.plugins.processor;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;


public class ServiceMapRelationship {

    private static final String MD5 = "MD5";

    /**
     * ThreadLocal object to generate hashes of relationships
     */
    private static ThreadLocal<MessageDigest> THREAD_LOCAL_MESSAGE_DIGEST = new ThreadLocal<>();

    /**
     * Service name for the relationship. This corresponds to the source of the relationship
     */
    private String serviceName;

    /**
     * Span kind for the relationship. This corresponds to the source span from the relationship
     */
    private String kind;

    /**
     * Destination for the relationship. Meant to be correlated to a "target" field on a separate relationship
     */
    private Endpoint destination;

    /**
     * Target for the relationship. Relates a "destination" from another relationship to a service name.
     */
    private Endpoint target;

    /**
     * Trace group name for this relationship
     */
    private String traceGroupName;

    /**
     * Deterministic hash id for this relationship
     */
    private String hashId;

    public ServiceMapRelationship() {
    }

    private ServiceMapRelationship(String serviceName, String kind, Endpoint destination, Endpoint target, String traceGroupName) {
        this.serviceName = serviceName;
        this.kind = kind;
        this.destination = destination;
        this.target = target;
        this.traceGroupName = traceGroupName;
        this.hashId = md5Hash();
    }

    /**
     * Create a destination relationship
     * @return Relationship with the fields set, and target set to null
     */
    public static ServiceMapRelationship newDestinationRelationship (
            final String serviceName,
            final String spanKind,
            final String resource,
            final String domain,
            final String traceGroupName) {
        return new ServiceMapRelationship(serviceName, spanKind, new Endpoint(resource, domain), null, traceGroupName);
    }

    /**
     * Create a target relationship
     * @return Relationship with the fields set, and destination set to null
     */
    public static ServiceMapRelationship newTargetRelationship (
            final String serviceName,
            final String spanKind,
            final String resource,
            final String domain,
            final String traceGroupName) {
        return new ServiceMapRelationship(serviceName, spanKind, null, new Endpoint(resource, domain), traceGroupName);
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public Endpoint getDestination() {
        return destination;
    }

    public void setDestination(Endpoint destination) {
        if(destination != null && target != null) {
            throw new RuntimeException("Cannot set both target and destination.");
        }
        this.destination = destination;
    }

    public Endpoint getTarget() {
        return target;
    }

    public void setTarget(Endpoint target) {
        if(target != null && destination != null) {
            throw new RuntimeException("Cannot set both target and destination.");
        }
        this.target = target;
    }

    public String getTraceGroupName() {
        return traceGroupName;
    }

    public void setTraceGroupName(String traceGroupName) {
        this.traceGroupName = traceGroupName;
    }

    public String getHashId() {
        return hashId;
    }

    public void setHashId(String hashId) {
        this.hashId = hashId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceMapRelationship that = (ServiceMapRelationship) o;
        return Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(kind, that.kind) &&
                Objects.equals(destination, that.destination) &&
                Objects.equals(target, that.target) &&
                Objects.equals(traceGroupName, that.traceGroupName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, kind, destination, target, traceGroupName);
    }

    private String unhashedString() {
        String result = serviceName + "," + kind + "," + traceGroupName + ",";
        if (target != null) {
            result += target.resource + "," + target.domain;
        }
        result += ",";
        if(destination != null) {
            result += destination.resource + "," + destination.domain;
        }
        return result;
    }

    private String md5Hash() {
        if(THREAD_LOCAL_MESSAGE_DIGEST.get() == null) {
            try {
                THREAD_LOCAL_MESSAGE_DIGEST.set(MessageDigest.getInstance(MD5));
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
        THREAD_LOCAL_MESSAGE_DIGEST.get().reset();
        THREAD_LOCAL_MESSAGE_DIGEST.get().update(unhashedString().getBytes());
        return Base64.encode(THREAD_LOCAL_MESSAGE_DIGEST.get().digest());
    }

    public static class Endpoint {
        private String resource;
        private String domain;

        public Endpoint(){}

        public Endpoint(final String resource, final String domain) {
            this.resource = resource;
            this.domain = domain;
        }

        public String getResource() {
            return resource;
        }

        public void setResource(String resource) {
            this.resource = resource;
        }

        public String getDomain() {
            return domain;
        }

        public void setDomain(String domain) {
            this.domain = domain;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Endpoint endpoint = (Endpoint) o;
            return Objects.equals(resource, endpoint.resource) &&
                    Objects.equals(domain, endpoint.domain);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resource, domain);
        }
    }
}
