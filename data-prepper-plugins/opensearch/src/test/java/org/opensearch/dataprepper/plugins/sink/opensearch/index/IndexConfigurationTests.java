/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.sink.opensearch.DistributionVersion;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.AWS_OPTION;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.DISTRIBUTION_VERSION;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.DOCUMENT_ROOT_KEY;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.DOCUMENT_VERSION_EXPRESSION;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.ROUTING;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.ROUTING_FIELD;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.SERVERLESS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.TEMPLATE_TYPE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConstants.RAW_DEFAULT_TEMPLATE_FILE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConstants.SERVICE_MAP_DEFAULT_TEMPLATE_FILE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConstants.TYPE_TO_DEFAULT_ALIAS;

@SuppressWarnings("unchecked")
public class IndexConfigurationTests {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String DEFAULT_TEMPLATE_FILE = "test-template-withshards.json";
    private static final String TEST_CUSTOM_INDEX_POLICY_FILE = "test-custom-index-policy-file.json";

    ObjectMapper objectMapper;

    @Test
    public void testRawAPMSpan() {
        final IndexConfiguration indexConfiguration = new IndexConfiguration.Builder().withIndexType(
                IndexType.TRACE_ANALYTICS_RAW.getValue()).build();
        assertThat(indexConfiguration.getIndexAlias(), equalTo(TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW)));
        assertThat(indexConfiguration.getIndexTemplate(), not(anEmptyMap()));
        assertThat(indexConfiguration.getIndexTemplate(), hasKey("mappings"));
        assertThat(indexConfiguration.getIndexTemplate().get("mappings"), instanceOf(Map.class));
        final Object dynamicTemplatesObj = ((Map<String, Object>) indexConfiguration.getIndexTemplate().get("mappings")).get("dynamic_templates");
        assertThat(dynamicTemplatesObj, instanceOf(List.class));
        final List<Map<String, Object>> dynamicTemplates = (List<Map<String, Object>>) dynamicTemplatesObj;

        assertThat(dynamicTemplates.size(), equalTo(2));
        assertThat(dynamicTemplates.get(0), hasKey("resource_attributes_map"));
        assertThat(dynamicTemplates.get(1), hasKey("span_attributes_map"));

    }

    @Test
    public void testServiceMap() {
        final IndexConfiguration indexConfiguration = new IndexConfiguration.Builder().withIndexType(
                IndexType.TRACE_ANALYTICS_SERVICE_MAP.getValue()).build();
        assertThat(indexConfiguration.getIndexAlias(), equalTo(TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_SERVICE_MAP)));
        assertThat(indexConfiguration.getIndexTemplate(), not(anEmptyMap()));
        assertThat(indexConfiguration.getIndexTemplate(), hasKey("mappings"));
        assertThat(indexConfiguration.getIndexTemplate().get("mappings"), instanceOf(Map.class));
        final Object dynamicTemplatesObj = ((Map<String, Object>) indexConfiguration.getIndexTemplate().get("mappings")).get("dynamic_templates");
        assertThat(dynamicTemplatesObj, instanceOf(List.class));
        final List<Map<String, Object>> dynamicTemplates = (List<Map<String, Object>>) dynamicTemplatesObj;

        assertThat(dynamicTemplates.size(), equalTo(1));
        assertThat(dynamicTemplates.get(0), hasKey("strings_as_keyword"));
    }

    @Test
    public void testRawAPMSpanWithIndexTemplates() {
        final IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
                .withIndexType(IndexType.TRACE_ANALYTICS_RAW.getValue())
                .withTemplateType(TemplateType.INDEX_TEMPLATE.getTypeName())
                .build();
        assertThat(indexConfiguration.getIndexAlias(), equalTo(TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW)));
        assertThat(indexConfiguration.getIndexTemplate(), not(anEmptyMap()));
        assertThat(indexConfiguration.getIndexTemplate(), hasKey("template"));
        assertThat(indexConfiguration.getIndexTemplate().get("template"), instanceOf(Map.class));
        final Object mappings = ((Map<String, Object>) indexConfiguration.getIndexTemplate().get("template")).get("mappings");
        assertThat(mappings, instanceOf(Map.class));
        final Object dynamicTemplatesObj = ((Map<String, Object>) mappings).get("dynamic_templates");
        assertThat(dynamicTemplatesObj, instanceOf(List.class));
        List<Map<String, Object>> dynamicTemplates = (List<Map<String, Object>>) dynamicTemplatesObj;

        assertThat(dynamicTemplates.size(), equalTo(2));
        assertThat(dynamicTemplates.get(0), hasKey("resource_attributes_map"));
        assertThat(dynamicTemplates.get(1), hasKey("span_attributes_map"));
    }

    @Test
    public void testServiceMapWithIndexTemplates() {
        final IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
                .withIndexType(IndexType.TRACE_ANALYTICS_SERVICE_MAP.getValue())
                .withTemplateType(TemplateType.INDEX_TEMPLATE.getTypeName())
                .build();
        assertThat(indexConfiguration.getIndexAlias(), equalTo(TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_SERVICE_MAP)));
        assertThat(indexConfiguration.getIndexTemplate(), not(anEmptyMap()));
        assertThat(indexConfiguration.getIndexTemplate(), hasKey("template"));
        assertThat(indexConfiguration.getIndexTemplate().get("template"), instanceOf(Map.class));
        final Object mappings = ((Map<String, Object>) indexConfiguration.getIndexTemplate().get("template")).get("mappings");
        assertThat(mappings, instanceOf(Map.class));
        final Object dynamicTemplatesObj = ((Map<String, Object>) mappings).get("dynamic_templates");
        assertThat(dynamicTemplatesObj, instanceOf(List.class));
        List<Map<String, Object>> dynamicTemplates = (List<Map<String, Object>>) dynamicTemplatesObj;

        assertThat(dynamicTemplates.size(), equalTo(1));
        assertThat(dynamicTemplates.get(0), hasKey("strings_as_keyword"));
    }

    @Test
    public void testValidCustom() throws MalformedURLException {
        final String defaultTemplateFilePath = Objects.requireNonNull(
                getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();

        final String testIndexAlias = "foo";
        IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
                .withIndexAlias(testIndexAlias)
                .withTemplateFile(defaultTemplateFilePath)
                .withIsmPolicyFile(TEST_CUSTOM_INDEX_POLICY_FILE)
                .withBulkSize(10)
                .withFlushTimeout(50)
                .build();

        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertEquals(10, indexConfiguration.getBulkSize());
        assertEquals(50, indexConfiguration.getFlushTimeout());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertNull(indexConfiguration.getQueryTerm());
        assertNull(indexConfiguration.getQueryWhen());
        assertNull(indexConfiguration.getQueryDuration());
        assertFalse(indexConfiguration.getQueryOnBulkFailures());

        indexConfiguration = new IndexConfiguration.Builder()
                .withIndexAlias(testIndexAlias)
                .withBulkSize(-1)
                .build();
        assertEquals(-1, indexConfiguration.getBulkSize());
    }

    @Test
    public void testValidCustom_from_s3() {
        final String testTemplateFilePath = "s3://folder/file.json";
        final String testS3AwsRegion = "us-east-2";
        final String testS3StsRoleArn = "arn:aws:iam::123456789:user/user-role";
        final String testS3StsExternalId = UUID.randomUUID().toString();
        final String fileContent = "{}";
        final long CONTENT_LENGTH = 3 * ONE_MB;

        final S3Client s3Client = mock(S3Client.class);

        final InputStream fileObjectStream = IOUtils.toInputStream(fileContent, StandardCharsets.UTF_8);
        final ResponseInputStream<GetObjectResponse> fileInputStream = new ResponseInputStream<>(
                GetObjectResponse.builder().contentLength(CONTENT_LENGTH).build(),
                AbortableInputStream.create(fileObjectStream)
        );
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(fileInputStream);

        final String testIndexAlias = UUID.randomUUID().toString();
        IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
            .withIndexAlias(testIndexAlias)
            .withTemplateFile(testTemplateFilePath)
            .withS3AwsRegion(testS3AwsRegion)
            .withS3AWSStsRoleArn(testS3StsRoleArn)
            .withS3AWSStsExternalId(testS3StsExternalId)
            .withS3Client(s3Client)
            .build();

        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertEquals(testS3AwsRegion, indexConfiguration.getS3AwsRegion());
        assertEquals(testS3StsRoleArn, indexConfiguration.getS3AwsStsRoleArn());
        assertEquals(testS3StsExternalId, indexConfiguration.getS3AwsStsExternalId());
    }

    @Test
    public void testValidCustomWithNoTemplateFile() {
        final String testIndexAlias = "foo";
        IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
                .withIndexAlias(testIndexAlias)
                .withBulkSize(10)
                .build();

        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertEquals(10, indexConfiguration.getBulkSize());
        assertTrue(indexConfiguration.getIndexTemplate().isEmpty());

        indexConfiguration = new IndexConfiguration.Builder()
                .withIndexAlias(testIndexAlias)
                .withBulkSize(-1)
                .build();
        assertEquals(-1, indexConfiguration.getBulkSize());
    }

    @Test
    public void testValidCustomWithNoTemplateFileButWithShardsAndReplicas() {
        final String testIndexAlias = "foo";
        IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
                .withIndexAlias(testIndexAlias)
                .withBulkSize(10)
                .withNumShards(5)
                .withNumReplicas(1)
                .build();

        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertEquals(10, indexConfiguration.getBulkSize());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertEquals(5, ((Map<String, Object>) indexConfiguration.getIndexTemplate().get(IndexConfiguration.SETTINGS)).get(IndexConfiguration.NUM_SHARDS));
        assertEquals(1, ((Map<String, Object>) indexConfiguration.getIndexTemplate().get(IndexConfiguration.SETTINGS)).get(IndexConfiguration.NUM_REPLICAS));


        indexConfiguration = new IndexConfiguration.Builder()
                .withIndexAlias(testIndexAlias)
                .withBulkSize(-1)
                .build();
        assertEquals(-1, indexConfiguration.getBulkSize());
    }

    @Test
    public void testValidCustomWithTemplateFileAndShards() {
        final String defaultTemplateFilePath = Objects.requireNonNull(
                getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
        final String testIndexAlias = "foo";
        IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
                .withIndexAlias(testIndexAlias)
                .withTemplateFile(defaultTemplateFilePath)
                .withBulkSize(10)
                .build();

        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertEquals(10, indexConfiguration.getBulkSize());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertEquals(100, ((Map<String, Object>) indexConfiguration.getIndexTemplate().get(IndexConfiguration.SETTINGS)).get(IndexConfiguration.NUM_SHARDS));
        assertEquals(2, ((Map<String, Object>) indexConfiguration.getIndexTemplate().get(IndexConfiguration.SETTINGS)).get(IndexConfiguration.NUM_REPLICAS));


        indexConfiguration = new IndexConfiguration.Builder()
                .withIndexAlias(testIndexAlias)
                .withBulkSize(-1)
                .build();
        assertEquals(-1, indexConfiguration.getBulkSize());
    }

    @Test
    public void testValidCustomWithTemplateContent() throws JsonProcessingException {
        final String testIndexAlias = "test";
        IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
                .withIndexAlias(testIndexAlias)
                .withTemplateContent(createTemplateContent())
                .withBulkSize(10)
                .build();

        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertEquals(10, indexConfiguration.getBulkSize());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertThat(indexConfiguration.getIndexTemplate(), equalTo(OBJECT_MAPPER.readValue(createTemplateContent(), new TypeReference<>() {})));
    }

    @Test
    public void readIndexConfigWithTemplateFileAndTemplateContentUsesTemplateContent() throws JsonProcessingException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig("custom", "test", "test-file", createTemplateContent(), null, null, null);

        final IndexConfiguration objectUnderTest = IndexConfiguration.readIndexConfig(openSearchSinkConfig);

        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getIndexTemplate(), notNullValue());
        assertThat(objectUnderTest.getIndexTemplate(), equalTo(OBJECT_MAPPER.readValue(createTemplateContent(), new TypeReference<>() {})));
    }

    @Test
    public void invalidTemplateContentThrowsInvalidPluginConfigurationException() throws JsonProcessingException {
        final String invalidTemplateContent = UUID.randomUUID().toString();

        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig("custom", null, null, invalidTemplateContent, null, null, null);

        assertThrows(InvalidPluginConfigurationException.class, () -> IndexConfiguration.readIndexConfig(openSearchSinkConfig));

    }

    @Test
    public void testInvalidCustom() {
        // Missing index alias
        final IndexConfiguration.Builder invalidBuilder = new IndexConfiguration.Builder();
        final Exception exception = assertThrows(IllegalStateException.class, invalidBuilder::build);
        assertEquals("Missing required properties:indexAlias", exception.getMessage());
    }

    @Test
    public void testReadIndexConfig_RawIndexType() throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.TRACE_ANALYTICS_RAW.getValue(), null, null, null, null, null, null);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        final URL expTemplateFile = indexConfiguration
                .getClass().getClassLoader().getResource(RAW_DEFAULT_TEMPLATE_FILE);
        assertEquals(IndexType.TRACE_ANALYTICS_RAW, indexConfiguration.getIndexType());
        assertEquals(TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW), indexConfiguration.getIndexAlias());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertEquals(5, indexConfiguration.getBulkSize());
        assertEquals(60_000L, indexConfiguration.getFlushTimeout());
        assertEquals(false, indexConfiguration.isEstimateBulkSizeUsingCompression());
        assertEquals(2, indexConfiguration.getMaxLocalCompressionsForEstimation());
        assertEquals("${spanId}", indexConfiguration.getDocumentId());
    }

    @Test
    public void testReadIndexConfig_InvalidIndexTypeValueString() throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(
                "i-am-an-illegitimate-index-type", null, null, null, null, null, null);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        assertThrows(IllegalArgumentException.class, () -> IndexConfiguration.readIndexConfig(openSearchSinkConfig));
    }

    @Test
    public void testReadIndexConfig_ServiceMapIndexType() throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.TRACE_ANALYTICS_SERVICE_MAP.getValue(), null, null, null, null, null, null);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        final URL expTemplateFile = indexConfiguration
                .getClass().getClassLoader().getResource(SERVICE_MAP_DEFAULT_TEMPLATE_FILE);
        assertEquals(IndexType.TRACE_ANALYTICS_SERVICE_MAP, indexConfiguration.getIndexType());
        assertEquals(TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_SERVICE_MAP), indexConfiguration.getIndexAlias());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertEquals(5, indexConfiguration.getBulkSize());
        assertEquals(60_000L, indexConfiguration.getFlushTimeout());
        assertEquals(false, indexConfiguration.isEstimateBulkSizeUsingCompression());
        assertEquals(2, indexConfiguration.getMaxLocalCompressionsForEstimation());
        assertEquals("${hashId}", indexConfiguration.getDocumentId());
    }

    @Test
    public void testReadIndexConfigCustom() throws JsonProcessingException {
        final String defaultTemplateFilePath = Objects.requireNonNull(
                getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
        final String testIndexAlias = "foo";
        final long testBulkSize = 10L;
        final long testFlushTimeout = 30_000L;
        final String testIdField = "someId";
        final Map<String,Object> metaData = initializeConfigMetaData(
                null, testIndexAlias, defaultTemplateFilePath, null, testBulkSize, testFlushTimeout, testIdField);
        metaData.put(IndexConfiguration.ESTIMATE_BULK_SIZE_USING_COMPRESSION, true);
        metaData.put(IndexConfiguration.MAX_LOCAL_COMPRESSIONS_FOR_ESTIMATION, 5);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metaData);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertEquals(testBulkSize, indexConfiguration.getBulkSize());
        assertEquals(testFlushTimeout, indexConfiguration.getFlushTimeout());
        assertEquals(true, indexConfiguration.isEstimateBulkSizeUsingCompression());
        assertEquals(5, indexConfiguration.getMaxLocalCompressionsForEstimation());
        assertEquals(testIdField, indexConfiguration.getDocumentId());
    }

    @Test
    public void testValidCustomWithQueryManager() {
        final String defaultTemplateFilePath = Objects.requireNonNull(
                getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();

        final Duration queryDuration = Duration.ofMinutes(3);
        final String queryTerm = UUID.randomUUID().toString();
        final String testIndexAlias = "foo";
        final String queryWhen = UUID.randomUUID().toString();
        IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
                .withIndexAlias(testIndexAlias)
                .withTemplateFile(defaultTemplateFilePath)
                .withIsmPolicyFile(TEST_CUSTOM_INDEX_POLICY_FILE)
                .withBulkSize(10)
                .withFlushTimeout(50)
                .withQueryOnIndexingFailure(true)
                .withQueryDuration(queryDuration)
                .withQueryTerm(queryTerm)
                .withQueryWhen(queryWhen)
                .build();

        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertEquals(10, indexConfiguration.getBulkSize());
        assertEquals(50, indexConfiguration.getFlushTimeout());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertEquals(queryTerm, indexConfiguration.getQueryTerm());
        assertEquals(queryWhen, indexConfiguration.getQueryWhen());
        assertEquals(queryDuration, indexConfiguration.getQueryDuration());
        assertTrue(indexConfiguration.getQueryOnBulkFailures());

        indexConfiguration = new IndexConfiguration.Builder()
                .withIndexAlias(testIndexAlias)
                .withBulkSize(-1)
                .build();
        assertEquals(-1, indexConfiguration.getBulkSize());
    }

    @Test
    public void testReadIndexConfig_ExplicitCustomIndexType() throws JsonProcessingException {
         final String defaultTemplateFilePath = Objects.requireNonNull(
                getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
        final String testIndexType = IndexType.CUSTOM.getValue();
        final String testIndexAlias = "foo";
        final long testBulkSize = 10L;
        final long testFlushTimeout = 30_000L;
        final String testIdField = "someId";
        final Map<String, Object> metadata = initializeConfigMetaData(
                testIndexType, testIndexAlias, defaultTemplateFilePath, null, testBulkSize, testFlushTimeout, testIdField);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertEquals(testBulkSize, indexConfiguration.getBulkSize());
        assertEquals(testFlushTimeout, indexConfiguration.getFlushTimeout());
        assertEquals(testIdField, indexConfiguration.getDocumentId());
    }

    @Test
    public void testReadIndexConfig_awsOptionServerlessDefault() throws JsonProcessingException {
        final String testIndexAlias = "foo";
        final Map<String, Object> metadata = initializeConfigMetaData(
                null, testIndexAlias, null, null, null, null, null);
        metadata.put(AWS_OPTION, Map.of(SERVERLESS, true));
        metadata.put(TEMPLATE_TYPE, TemplateType.V1.getTypeName());
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        assertEquals(IndexType.MANAGEMENT_DISABLED, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    }

    @Test
    public void testReadIndexConfig_awsServerlessIndexTypeOverride() throws JsonProcessingException {
        final String testIndexAlias = "foo";
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.CUSTOM.getValue(), testIndexAlias, null, null, null, null, null);
        metadata.put(AWS_OPTION, Map.of(SERVERLESS, true));
        metadata.put(TEMPLATE_TYPE, TemplateType.V1.getTypeName());
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertEquals(TemplateType.INDEX_TEMPLATE, indexConfiguration.getTemplateType());
        assertEquals(true, indexConfiguration.getServerless());
    }

    @Test
    public void testReadIndexConfig_distributionVersionDefault() throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(
                null, "foo", null,null, null, null, null);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        assertEquals(indexConfiguration.getDistributionVersion(), DistributionVersion.DEFAULT);
    }

    @Test
    public void testReadIndexConfig_es6Override() throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(
                null, "foo", null, null, null, null, null);
        metadata.put(DISTRIBUTION_VERSION, "es6");
        metadata.put(TEMPLATE_TYPE, TemplateType.INDEX_TEMPLATE.getTypeName());
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        assertEquals(indexConfiguration.getDistributionVersion(), DistributionVersion.ES6);
        assertEquals(TemplateType.V1, indexConfiguration.getTemplateType());
        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
    }

    @Test
    public void testReadIndexConfig_documentRootKey() throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(
            IndexType.CUSTOM.getValue(), "foo", null, null, null, null, null);
        final String expectedRootKey = UUID.randomUUID().toString();
        metadata.put(DOCUMENT_ROOT_KEY, expectedRootKey);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        assertEquals(expectedRootKey, indexConfiguration.getDocumentRootKey());
    }

    @Test
    public void testReadIndexConfig_emptyDocumentRootKey() throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(
            IndexType.CUSTOM.getValue(), "foo", null, null, null, null, null);
        metadata.put(DOCUMENT_ROOT_KEY, "");
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        assertThrows(IllegalArgumentException.class, () -> IndexConfiguration.readIndexConfig(openSearchSinkConfig));
    }

    @Test
    public void testReadIndexConfig_routing() throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.CUSTOM.getValue(), "foo", null, null, null, null, null);
        final String expectedRoutingValue = UUID.randomUUID().toString();
        metadata.put(ROUTING, expectedRoutingValue);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        assertEquals(expectedRoutingValue, indexConfiguration.getRouting());
    }

    @Test
    public void testReadIndexConfig_routingField() throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.CUSTOM.getValue(), "foo", null, null, null, null, null);
        final String expectedRoutingFieldValue = UUID.randomUUID().toString();
        metadata.put(ROUTING_FIELD, expectedRoutingFieldValue);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        assertEquals(expectedRoutingFieldValue, indexConfiguration.getRoutingField());
    }

    @ParameterizedTest
    @ValueSource(strings = {"${key}", "${getMetadata(\"key\")}"})
    public void testReadIndexConfig_withValidDocumentVersionExpression(final String versionExpression) throws JsonProcessingException {

        final ExpressionEvaluator expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidFormatExpression(versionExpression)).thenReturn(true);

        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.CUSTOM.getValue(), "foo", null, null, null, null, null);
        metadata.put(DOCUMENT_VERSION_EXPRESSION, versionExpression);

        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);

        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig, expressionEvaluator);

        assertThat(indexConfiguration, notNullValue());
        assertThat(indexConfiguration.getVersionExpression(), equalTo(versionExpression));
    }

    @Test
    public void testReadIndexConfig_withInvalidDocumentVersionExpression_throws_InvalidPluginConfigurationException() throws JsonProcessingException {
        final String versionExpression = UUID.randomUUID().toString();

        final ExpressionEvaluator expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidFormatExpression(versionExpression)).thenReturn(false);

        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.CUSTOM.getValue(), "foo", null, null, null, null, null);
        metadata.put(DOCUMENT_VERSION_EXPRESSION, versionExpression);

        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);

        assertThrows(InvalidPluginConfigurationException.class, () -> IndexConfiguration.readIndexConfig(openSearchSinkConfig, expressionEvaluator));
    }

    @Test
    void getTemplateType_defaults_to_V1() throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.CUSTOM.getValue(), "foo", null, null, null, null, null);
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        assertThat(indexConfiguration.getTemplateType(), equalTo(TemplateType.V1));
    }

    @ParameterizedTest
    @EnumSource(TemplateType.class)
    void getTemplateType_with_configured_templateType(final TemplateType templateType) throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.CUSTOM.getValue(), "foo", null, null, null, null, null);
        metadata.put(TEMPLATE_TYPE, templateType.getTypeName());
        final OpenSearchSinkConfig openSearchSinkConfig = getOpenSearchSinkConfig(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(openSearchSinkConfig);
        assertThat(indexConfiguration.getTemplateType(), equalTo(templateType));
    }

    private OpenSearchSinkConfig generateOpenSearchSinkConfig(
            final String indexType, final String indexAlias, final String templateFilePath, final String templateContent,
            final Long bulkSize, final Long flushTimeout, final String documentIdField) throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigMetaData(indexType, indexAlias, templateFilePath, templateContent, bulkSize, flushTimeout, documentIdField);
        return getOpenSearchSinkConfig(metadata);
    }

    private OpenSearchSinkConfig getOpenSearchSinkConfig(Map<String, Object> metadata) throws JsonProcessingException {
        objectMapper = new ObjectMapper();
        String json = new ObjectMapper().writeValueAsString(metadata);
        OpenSearchSinkConfig openSearchSinkConfig = objectMapper.readValue(json, OpenSearchSinkConfig.class);

        return openSearchSinkConfig;
    }

    private Map<String, Object> initializeConfigMetaData(
            String indexType, String indexAlias, String templateFilePath, String templateContent, Long bulkSize, Long flushTimeout, String documentId) {
        final Map<String, Object> metadata = new HashMap<>();
        if (indexType != null) {
            metadata.put(IndexConfiguration.INDEX_TYPE, indexType);
        }
        if (indexAlias != null) {
            metadata.put(IndexConfiguration.INDEX_ALIAS, indexAlias);
        }
        if (templateFilePath != null) {
            metadata.put(IndexConfiguration.TEMPLATE_FILE, templateFilePath);
        }

        if (templateContent != null) {
            metadata.put(IndexConfiguration.TEMPLATE_CONTENT, templateContent);
        }

        if (bulkSize != null) {
            metadata.put(IndexConfiguration.BULK_SIZE, bulkSize);
        }
        if (flushTimeout != null) {
            metadata.put(IndexConfiguration.FLUSH_TIMEOUT, flushTimeout);
        }
        if (documentId != null) {
            metadata.put(IndexConfiguration.DOCUMENT_ID, documentId);
        }
        return metadata;
    }

    private String createTemplateContent() {
        return "{\"index_patterns\":[\"test-*\"]," +
                "\"template\":{\"aliases\":{\"my_test_logs\":{}}," +
                "\"settings\":{\"number_of_shards\":5,\"number_of_replicas\":2,\"refresh_interval\":-1}," +
                "\"mappings\":{\"properties\":{\"timestamp\":{\"type\":\"date\",\"format\":\"yyyy-MM-ddHH:mm:ss||yyyy-MM-dd||epoch_millis\"}," +
                "\"value\":{\"type\":\"double\"}}}}}";
    }
}
