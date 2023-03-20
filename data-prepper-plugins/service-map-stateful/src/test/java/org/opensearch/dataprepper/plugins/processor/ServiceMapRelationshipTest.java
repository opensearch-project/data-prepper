/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class ServiceMapRelationshipTest {
    @Test
    void newDestinationRelationship_hash_is_consistent_with_static_known_values() {
        final String serviceName = "ServiceName";
        final String kind = "Kind";
        final String traceGroupName = "TraceGroupName";

        final ServiceMapRelationship objectUnderTest = ServiceMapRelationship.newDestinationRelationship(serviceName, kind, "d1", "r1", traceGroupName);

        assertThat(objectUnderTest.getHashId(), equalTo("r7rZwbptLFxOLPUlg/x2nA=="));
    }

    @Test
    void newTargetRelationship_hash_is_consistent_with_static_known_values() {
        final String serviceName = "ServiceName";
        final String kind = "Kind";
        final String traceGroupName = "TraceGroupName";

        final ServiceMapRelationship objectUnderTest = ServiceMapRelationship.newTargetRelationship(serviceName, kind, "d1", "r1", traceGroupName);

        assertThat(objectUnderTest.getHashId(), equalTo("lJtcAzLqaaeyhym6a99CcA=="));
    }

    @Nested
    class WithRandomValues {
        private String serviceName;
        private String kind;
        private String traceGroupName;
        private String domain;
        private String resource;

        @BeforeEach
        void setUp() {
            serviceName = UUID.randomUUID().toString();
            kind = UUID.randomUUID().toString();
            traceGroupName = UUID.randomUUID().toString();
            domain = UUID.randomUUID().toString();
            resource = UUID.randomUUID().toString();
        }

        @Test
        void newDestinationRelationship_with_equalTo() {
            final ServiceMapRelationship objectUnderTest = ServiceMapRelationship.newDestinationRelationship(serviceName, kind, domain, resource, traceGroupName);
            final ServiceMapRelationship otherObject = ServiceMapRelationship.newDestinationRelationship(serviceName, kind, domain, resource, traceGroupName);

            assertThat(objectUnderTest, equalTo(otherObject));
        }

        @Test
        void newTargetRelationship_with_equalTo() {
            final ServiceMapRelationship objectUnderTest = ServiceMapRelationship.newTargetRelationship(serviceName, kind, domain, resource, traceGroupName);
            final ServiceMapRelationship otherObject = ServiceMapRelationship.newTargetRelationship(serviceName, kind, domain, resource, traceGroupName);

            assertThat(objectUnderTest, equalTo(otherObject));
        }

        @Test
        void newIsolatedService_with_equalTo() {
            final ServiceMapRelationship objectUnderTest = ServiceMapRelationship.newIsolatedService(serviceName, traceGroupName);
            final ServiceMapRelationship otherObject = ServiceMapRelationship.newIsolatedService(serviceName, traceGroupName);

            assertThat(objectUnderTest, equalTo(otherObject));
        }

        @Test
        void newDestinationRelationship_json_serialization_and_deserialization() throws JsonProcessingException {
            final ObjectMapper objectMapper = new ObjectMapper();
            final String jsonString = objectMapper.writeValueAsString(ServiceMapRelationship.newDestinationRelationship(serviceName, kind, domain, resource, traceGroupName));

            final ServiceMapRelationship deserializedObject = objectMapper.readValue(jsonString, ServiceMapRelationship.class);

            assertThat(deserializedObject.getServiceName(), equalTo(serviceName));
            assertThat(deserializedObject.getKind(), equalTo(kind));
            assertThat(deserializedObject.getTraceGroupName(), equalTo(traceGroupName));
            assertThat(deserializedObject.getDestination(), notNullValue());
            assertThat(deserializedObject.getDestination().getDomain(), equalTo(domain));
            assertThat(deserializedObject.getDestination().getResource(), equalTo(resource));
            assertThat(deserializedObject.getTarget(), nullValue());
        }

        @Test
        void newTargetRelationship_json_serialization_and_deserialization() throws JsonProcessingException {
            final ObjectMapper objectMapper = new ObjectMapper();
            final String jsonString = objectMapper.writeValueAsString(ServiceMapRelationship.newTargetRelationship(serviceName, kind, domain, resource, traceGroupName));

            final ServiceMapRelationship deserializedObject = objectMapper.readValue(jsonString, ServiceMapRelationship.class);

            assertThat(deserializedObject.getServiceName(), equalTo(serviceName));
            assertThat(deserializedObject.getKind(), equalTo(kind));
            assertThat(deserializedObject.getTraceGroupName(), equalTo(traceGroupName));
            assertThat(deserializedObject.getDestination(), nullValue());
            assertThat(deserializedObject.getTarget(), notNullValue());
            assertThat(deserializedObject.getTarget().getDomain(), equalTo(domain));
            assertThat(deserializedObject.getTarget().getResource(), equalTo(resource));
        }

        @Test
        void newIsolatedService_json_serialization_and_deserialization() throws JsonProcessingException {
            final ObjectMapper objectMapper = new ObjectMapper();
            final String jsonString = objectMapper.writeValueAsString(ServiceMapRelationship.newIsolatedService(serviceName, traceGroupName));

            final ServiceMapRelationship deserializedObject = objectMapper.readValue(jsonString, ServiceMapRelationship.class);

            assertThat(deserializedObject.getServiceName(), equalTo(serviceName));
            assertThat(deserializedObject.getTraceGroupName(), equalTo(traceGroupName));
            assertThat(deserializedObject.getDestination(), nullValue());
            assertThat(deserializedObject.getTarget(), nullValue());
        }
    }
}