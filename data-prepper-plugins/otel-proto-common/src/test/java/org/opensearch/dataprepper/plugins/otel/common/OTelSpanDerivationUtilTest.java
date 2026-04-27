/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.common;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OTelSpanDerivationUtilTest {

    @Test
    void testComputeErrorAndFault_Http500() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.response.status_code", 500);

        OTelSpanDerivationUtil.ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault((String) null, spanAttributes);

        assertEquals(1, result.fault);
        assertEquals(0, result.error);
    }

    @Test
    void testComputeErrorAndFault_Http404() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.response.status_code", 404);

        OTelSpanDerivationUtil.ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault((String) null, spanAttributes);

        assertEquals(0, result.fault);
        assertEquals(1, result.error);
    }

    @Test
    void testComputeErrorAndFault_Http200() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.response.status_code", 200);

        OTelSpanDerivationUtil.ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault((String) null, spanAttributes);

        assertEquals(0, result.fault);
        assertEquals(0, result.error);
    }

    @Test
    void testComputeErrorAndFault_SpanStatusError() {
        OTelSpanDerivationUtil.ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault("ERROR", null);

        assertEquals(1, result.fault);
        assertEquals(0, result.error);
    }

    @Test
    void testComputeErrorAndFault_MapVersion() {
        Map<String, Object> spanStatus = new HashMap<>();
        spanStatus.put("code", "ERROR");

        OTelSpanDerivationUtil.ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault(spanStatus, null);

        assertEquals(1, result.fault);
        assertEquals(0, result.error);
    }

    @Test
    void testComputeOperationName_HttpDerivation() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.request.method", "GET");
        spanAttributes.put("http.path", "/payment/123");

        String result = OTelSpanDerivationUtil.computeOperationName("GET", spanAttributes);

        assertEquals("GET /payment", result);
    }

    @Test
    void testComputeOperationName_NoHttpDerivation() {
        Map<String, Object> spanAttributes = new HashMap<>();

        String result = OTelSpanDerivationUtil.computeOperationName("CustomOperation", spanAttributes);

        assertEquals("CustomOperation", result);
    }

    @Test
    void testComputeOperationName_FallbackToHttpMethod() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.method", "POST");
        spanAttributes.put("http.path", "/api/users");

        String result = OTelSpanDerivationUtil.computeOperationName("POST", spanAttributes);

        assertEquals("POST /api", result);
    }

    @Test
    void testComputeOperationName_UnknownOperation() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.request.method", "GET");

        String result = OTelSpanDerivationUtil.computeOperationName("GET", spanAttributes);

        assertEquals("UnknownOperation", result);
    }

    @Test
    void testComputeEnvironment_DeploymentEnvironmentName() {
        Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("deployment.environment.name", "production");

        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);

        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", resource);

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("production", result);
    }

    @Test
    void testComputeEnvironment_DeploymentEnvironment() {
        Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("deployment.environment", "staging");

        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);

        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", resource);

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("staging", result);
    }

    @Test
    void testComputeEnvironment_Default() {
        String result = OTelSpanDerivationUtil.computeEnvironment(null);

        assertEquals("generic:default", result);
    }

    @Test
    void testComputeEnvironment_NoResourceAttributes() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", new HashMap<>());

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("generic:default", result);
    }

    @Test
    void testComputeEnvironment_EmptySpanAttributes() {
        Map<String, Object> spanAttributes = new HashMap<>();

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("generic:default", result);
    }

    @Test
    void testComputeEnvironment_ResourceNotMap() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", "not-a-map");

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("generic:default", result);
    }

    @Test
    void testComputeEnvironment_AttributesNotMap() {
        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", "not-a-map");

        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", resource);

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("generic:default", result);
    }

    @Test
    void testComputeEnvironment_PriorityDeploymentEnvironmentName() {
        Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("deployment.environment.name", "production");
        resourceAttributes.put("deployment.environment", "staging");

        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);

        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", resource);

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("production", result);
    }

    @Test
    void testComputeEnvironment_NullResourceAttributes() {
        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", null);

        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", resource);

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("generic:default", result);
    }

    @Test
    void testComputeEnvironment_AwsApiGateway() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("cloud.platform", "aws_api_gateway");
        spanAttributes.put("aws.api_gateway.stage", "prod");

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("api-gateway:prod", result);
    }

    @Test
    void testComputeEnvironment_AwsEc2() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("cloud.platform", "aws_ec2");

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("ec2:default", result);
    }

    @Test
    void testComputeEnvironment_AwsLambdaFromCloudResourceId() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("cloud.resource_id", "arn:aws:lambda:us-east-1:123456789012:function:my-function");

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("lambda:default", result);
    }

    @Test
    void testComputeEnvironment_AwsLambdaFromInvokedArn() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("aws.lambda.invoked_arn", "arn:aws:lambda:us-east-1:123456789012:function:my-function");

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("lambda:default", result);
    }

    @Test
    void testComputeEnvironment_AwsLambdaFromResourceAttributes() {
        Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("cloud.provider", "aws");
        resourceAttributes.put("faas.name", "my-lambda-function");

        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);

        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", resource);

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("lambda:default", result);
    }

    @Test
    void testComputeEnvironment_AwsServicePriorityOverDeployment() {
        Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("deployment.environment.name", "production");

        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);

        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("cloud.platform", "aws_ec2");
        spanAttributes.put("resource", resource);

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("ec2:default", result);
    }


    @Test
    void testParseHttpStatusCode() {
        assertEquals(Integer.valueOf(200), OTelSpanDerivationUtil.parseHttpStatusCode(200));
        assertEquals(Integer.valueOf(404), OTelSpanDerivationUtil.parseHttpStatusCode("404"));
        assertEquals(Integer.valueOf(500), OTelSpanDerivationUtil.parseHttpStatusCode(500L));
        assertNull(OTelSpanDerivationUtil.parseHttpStatusCode("invalid"));
        assertNull(OTelSpanDerivationUtil.parseHttpStatusCode(null));
    }


    @Test
    void testGetStringAttribute() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", 123);
        attributes.put("key3", null);

        assertEquals("value1", OTelSpanDerivationUtil.getStringAttribute(attributes, "key1"));
        assertEquals("123", OTelSpanDerivationUtil.getStringAttribute(attributes, "key2"));
        assertNull(OTelSpanDerivationUtil.getStringAttribute(attributes, "key3"));
        assertNull(OTelSpanDerivationUtil.getStringAttribute(attributes, "nonexistent"));
        assertNull(OTelSpanDerivationUtil.getStringAttribute(null, "key1"));
    }

    @Test
    void testGetStringAttributeFromMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", 123);

        assertEquals("value1", OTelSpanDerivationUtil.getStringAttributeFromMap(map, "key1"));
        assertEquals("123", OTelSpanDerivationUtil.getStringAttributeFromMap(map, "key2"));
        assertNull(OTelSpanDerivationUtil.getStringAttributeFromMap(null, "key1"));
    }


    @Test
    void testErrorFaultResult() {
        OTelSpanDerivationUtil.ErrorFaultResult result = new OTelSpanDerivationUtil.ErrorFaultResult(1, 0);
        assertEquals(1, result.error);
        assertEquals(0, result.fault);
    }

    @Test
    void testComputeRemoteOperationAndService_RpcAwsApi() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("rpc.service", "DynamoDb");
        spanAttributes.put("rpc.system", "aws-api");
        spanAttributes.put("rpc.method", "PutItem");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("PutItem", result.getOperation());
        assertEquals("AWS::DynamoDB", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_RpcMethod() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("rpc.method", "GetItem");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("GetItem", result.getOperation());
        assertEquals("UnknownRemoteService", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_DbSystem() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("db.system", "postgresql");
        spanAttributes.put("db.operation", "SELECT");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("SELECT", result.getOperation());
        assertEquals("postgresql", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_DbStatement() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("db.system", "mysql");
        spanAttributes.put("db.statement", "INSERT INTO users VALUES (1, 'test')");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("INSERT", result.getOperation());
        assertEquals("mysql", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_FaaS() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("faas.invoked_name", "my-lambda-function");
        spanAttributes.put("faas.trigger", "http");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("http", result.getOperation());
        assertEquals("my-lambda-function", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_Messaging() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("messaging.system", "kafka");
        spanAttributes.put("messaging.operation", "publish");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("publish", result.getOperation());
        assertEquals("kafka", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_GraphQL() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("graphql.operation.type", "query");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("query", result.getOperation());
        assertEquals("graphql", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_PeerService() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("peer.service", "payment-service");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("UnknownRemoteOperation", result.getOperation());
        assertEquals("payment-service", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_ServerAddress() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("server.address", "api.example.com");
        spanAttributes.put("server.port", "443");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("UnknownRemoteOperation", result.getOperation());
        assertEquals("api.example.com:443", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_NetPeerName() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("net.peer.name", "db.example.com");
        spanAttributes.put("net.peer.port", "5432");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("UnknownRemoteOperation", result.getOperation());
        assertEquals("db.example.com:5432", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_UrlFull() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("url.full", "https://api.example.com:8080/v1/users");
        spanAttributes.put("http.request.method", "POST");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("POST /v1", result.getOperation());
        assertEquals("api.example.com:8080", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_HttpUrl() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.url", "http://service.local:3000/api/data");
        spanAttributes.put("http.method", "GET");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("GET /api", result.getOperation());
        assertEquals("service.local:3000", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_EmptyAttributes() {
        Map<String, Object> spanAttributes = new HashMap<>();

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("UnknownRemoteOperation", result.getOperation());
        assertEquals("UnknownRemoteService", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_NullAttributes() {
        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(null);

        assertEquals("UnknownRemoteOperation", result.getOperation());
        assertEquals("UnknownRemoteService", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_AwsServiceMappings() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("rpc.service", "AmazonSNS");
        spanAttributes.put("rpc.system", "aws-api");
        spanAttributes.put("rpc.method", "GetItem");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("GetItem", result.getOperation());
        assertEquals("AWS::SNS", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_DbSystemName() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("db.system.name", "redis");
        spanAttributes.put("db.operation.name", "GET");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("GET", result.getOperation());
        assertEquals("redis", result.getService());
    }

    @Test
    void testComputeRemoteOperationAndService_NetworkPeerAddress() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("network.peer.address", "10.0.0.1");
        spanAttributes.put("network.peer.port", "8080");

        RemoteOperationAndService result = OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);

        assertEquals("UnknownRemoteOperation", result.getOperation());
        assertEquals("10.0.0.1:8080", result.getService());
    }
}
