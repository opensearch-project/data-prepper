/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Objects;


public class ServiceMapRelationship {

    private static final String MD5 = "MD5";

    /**
     * ThreadLocal object to generate hashes of relationships
     */
    private static final ThreadLocal<MessageDigest> THREAD_LOCAL_MESSAGE_DIGEST = new ThreadLocal<>();

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

    private ServiceMapRelationship() { }

    private ServiceMapRelationship(final String serviceName, final String kind, final Endpoint destination, final Endpoint target, final String traceGroupName) {
        this.serviceName = serviceName;
        this.kind = kind;
        this.destination = destination;
        this.target = target;
        this.traceGroupName = traceGroupName;
        this.hashId = md5Hash();
        System.out.println("----"+serviceName+"----"+kind+"----"+destination+"----"+target+"---"+traceGroupName);
    }

    /**
     * Create a destination relationship
     * @param serviceName service name
     * @param spanKind span kind
     * @param domain domain
     * @param resource resource
     * @param traceGroupName trace group name
     * @return Relationship with the fields set, and target set to null
     */
    public static ServiceMapRelationship newDestinationRelationship (
            final String serviceName,
            final String spanKind,
            final String domain,
            final String resource,
            final String traceGroupName) {

        System.out.println("--DR--"+serviceName+"----"+spanKind+"----"+resource+"---dom--"+domain+"---"+traceGroupName);

        return new ServiceMapRelationship(serviceName, spanKind, new Endpoint(resource, domain), null, traceGroupName);
    }

    /**
     * Create a target relationship
     * @param serviceName service name
     * @param spanKind span kind
     * @param domain domain
     * @param resource resource
     * @param traceGroupName trace group name
     * @return Relationship with the fields set, and destination set to null
     */
    public static ServiceMapRelationship newTargetRelationship (
            final String serviceName,
            final String spanKind,
            final String domain,
            final String resource,
            final String traceGroupName) {
        System.out.println("-TR---"+serviceName+"----"+spanKind+"----"+resource+"---dom--"+domain+"---"+traceGroupName);

        return new ServiceMapRelationship(serviceName, spanKind, null, new Endpoint(resource, domain), traceGroupName);
    }

    public static ServiceMapRelationship newIsolatedService (
            final String serviceName,
            final String traceGroupName) {
        return new ServiceMapRelationship(serviceName, null, null, null, traceGroupName);
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getKind() {
        return kind;
    }

    public Endpoint getDestination() {
        return destination;
    }

    public Endpoint getTarget() {
        return target;
    }

    public String getTraceGroupName() {
        return traceGroupName;
    }

    public String getHashId() {
        return hashId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ServiceMapRelationship that = (ServiceMapRelationship) o;
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
            } catch (final NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
        THREAD_LOCAL_MESSAGE_DIGEST.get().reset();
        THREAD_LOCAL_MESSAGE_DIGEST.get().update(unhashedString().getBytes());
        return Base64.getEncoder().encodeToString(THREAD_LOCAL_MESSAGE_DIGEST.get().digest());
    }

    /**
     * The endpoint follows the URL spec.
     * <p> Example, https://paymentservice/makePayment.  <p>
     *  domain: paymentservice
     *  resource: makePayment
     * 
     *
     */
    public static class Endpoint {
        private final String resource;
        private final String domain;

        @JsonCreator
        public Endpoint(
                @JsonProperty("resource") final String resource,
                @JsonProperty("domain") final String domain) {
            this.resource = resource;
            this.domain = domain;
        }

        public String getResource() {
            return resource;
        }

        public String getDomain() {
            return domain;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Endpoint endpoint = (Endpoint) o;
            return Objects.equals(resource, endpoint.resource) &&
                    Objects.equals(domain, endpoint.domain);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resource, domain);
        }
    }
}
