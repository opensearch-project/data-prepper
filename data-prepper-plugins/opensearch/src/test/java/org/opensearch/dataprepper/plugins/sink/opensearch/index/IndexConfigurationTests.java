/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.opensearch.DistributionVersion;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.AWS_OPTION;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.DISTRIBUTION_VERSION;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.DOCUMENT_ROOT_KEY;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.SERVERLESS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration.TEMPLATE_TYPE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConstants.RAW_DEFAULT_TEMPLATE_FILE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConstants.SERVICE_MAP_DEFAULT_TEMPLATE_FILE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConstants.TYPE_TO_DEFAULT_ALIAS;

@SuppressWarnings("unchecked")
public class IndexConfigurationTests {
    private static final String DEFAULT_TEMPLATE_FILE = "test-template-withshards.json";
    private static final String TEST_CUSTOM_INDEX_POLICY_FILE = "test-custom-index-policy-file.json";

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
    public void testValidCustomWithNoTemplateFile() throws MalformedURLException {
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
    public void testInvalidCustom() {
        // Missing index alias
        final IndexConfiguration.Builder invalidBuilder = new IndexConfiguration.Builder();
        final Exception exception = assertThrows(IllegalStateException.class, invalidBuilder::build);
        assertEquals("Missing required properties:indexAlias", exception.getMessage());
    }

    @Test
    public void testReadIndexConfig_RawIndexType() {
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.TRACE_ANALYTICS_RAW.getValue(), null, null, null, null, null);
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
        final URL expTemplateFile = indexConfiguration
                .getClass().getClassLoader().getResource(RAW_DEFAULT_TEMPLATE_FILE);
        assertEquals(IndexType.TRACE_ANALYTICS_RAW, indexConfiguration.getIndexType());
        assertEquals(TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW), indexConfiguration.getIndexAlias());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertEquals(5, indexConfiguration.getBulkSize());
        assertEquals(60_000L, indexConfiguration.getFlushTimeout());
        assertEquals(false, indexConfiguration.isEstimateBulkSizeUsingCompression());
        assertEquals(2, indexConfiguration.getMaxLocalCompressionsForEstimation());
        assertEquals("spanId", indexConfiguration.getDocumentIdField());
    }

    @Test
    public void testReadIndexConfig_InvalidIndexTypeValueString() {
        final Map<String, Object> metadata = initializeConfigMetaData(
                "i-am-an-illegitimate-index-type", null, null, null, null, null);
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        assertThrows(IllegalArgumentException.class, () -> IndexConfiguration.readIndexConfig(pluginSetting));
    }

    @Test
    public void testReadIndexConfig_ServiceMapIndexType() {
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.TRACE_ANALYTICS_SERVICE_MAP.getValue(), null, null, null, null, null);
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
        final URL expTemplateFile = indexConfiguration
                .getClass().getClassLoader().getResource(SERVICE_MAP_DEFAULT_TEMPLATE_FILE);
        assertEquals(IndexType.TRACE_ANALYTICS_SERVICE_MAP, indexConfiguration.getIndexType());
        assertEquals(TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_SERVICE_MAP), indexConfiguration.getIndexAlias());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertEquals(5, indexConfiguration.getBulkSize());
        assertEquals(60_000L, indexConfiguration.getFlushTimeout());
        assertEquals(false, indexConfiguration.isEstimateBulkSizeUsingCompression());
        assertEquals(2, indexConfiguration.getMaxLocalCompressionsForEstimation());
        assertEquals("hashId", indexConfiguration.getDocumentIdField());
    }

    @Test
    public void testReadIndexConfigCustom() {
        final String defaultTemplateFilePath = Objects.requireNonNull(
                getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
        final String testIndexAlias = "foo";
        final long testBulkSize = 10L;
        final long testFlushTimeout = 30_000L;
        final String testIdField = "someId";
        final PluginSetting pluginSetting = generatePluginSetting(
                null, testIndexAlias, defaultTemplateFilePath, testBulkSize, testFlushTimeout, testIdField);
        pluginSetting.getSettings().put(IndexConfiguration.ESTIMATE_BULK_SIZE_USING_COMPRESSION, true);
        pluginSetting.getSettings().put(IndexConfiguration.MAX_LOCAL_COMPRESSIONS_FOR_ESTIMATION, 5);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertEquals(testBulkSize, indexConfiguration.getBulkSize());
        assertEquals(testFlushTimeout, indexConfiguration.getFlushTimeout());
        assertEquals(true, indexConfiguration.isEstimateBulkSizeUsingCompression());
        assertEquals(5, indexConfiguration.getMaxLocalCompressionsForEstimation());
        assertEquals(testIdField, indexConfiguration.getDocumentIdField());
    }

    @Test
    public void testReadIndexConfig_ExplicitCustomIndexType() {
        final String defaultTemplateFilePath = Objects.requireNonNull(
                getClass().getClassLoader().getResource(DEFAULT_TEMPLATE_FILE)).getFile();
        final String testIndexType = IndexType.CUSTOM.getValue();
        final String testIndexAlias = "foo";
        final long testBulkSize = 10L;
        final long testFlushTimeout = 30_000L;
        final String testIdField = "someId";
        final Map<String, Object> metadata = initializeConfigMetaData(
                testIndexType, testIndexAlias, defaultTemplateFilePath, testBulkSize, testFlushTimeout, testIdField);
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertFalse(indexConfiguration.getIndexTemplate().isEmpty());
        assertEquals(testBulkSize, indexConfiguration.getBulkSize());
        assertEquals(testFlushTimeout, indexConfiguration.getFlushTimeout());
        assertEquals(testIdField, indexConfiguration.getDocumentIdField());
    }

    @Test
    public void testReadIndexConfig_awsOptionServerlessDefault() {
        final String testIndexAlias = "foo";
        final Map<String, Object> metadata = initializeConfigMetaData(
                null, testIndexAlias, null, null, null, null);
        metadata.put(AWS_OPTION, Map.of(SERVERLESS, true));
        metadata.put(TEMPLATE_TYPE, TemplateType.V1.getTypeName());
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
        assertEquals(IndexType.MANAGEMENT_DISABLED, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
    }

    @Test
    public void testReadIndexConfig_awsServerlessIndexTypeOverride() {
        final String testIndexAlias = "foo";
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.CUSTOM.getValue(), testIndexAlias, null, null, null, null);
        metadata.put(AWS_OPTION, Map.of(SERVERLESS, true));
        metadata.put(TEMPLATE_TYPE, TemplateType.V1.getTypeName());
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
        assertEquals(testIndexAlias, indexConfiguration.getIndexAlias());
        assertEquals(TemplateType.INDEX_TEMPLATE, indexConfiguration.getTemplateType());
        assertEquals(true, indexConfiguration.getServerless());
    }

    @Test
    public void testReadIndexConfig_distributionVersionDefault() {
        final Map<String, Object> metadata = initializeConfigMetaData(
                null, "foo", null, null, null, null);
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
        assertEquals(indexConfiguration.getDistributionVersion(), DistributionVersion.DEFAULT);
    }

    @Test
    public void testReadIndexConfig_es6Override() {
        final Map<String, Object> metadata = initializeConfigMetaData(
                null, "foo", null, null, null, null);
        metadata.put(DISTRIBUTION_VERSION, "es6");
        metadata.put(TEMPLATE_TYPE, TemplateType.INDEX_TEMPLATE.getTypeName());
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
        assertEquals(indexConfiguration.getDistributionVersion(), DistributionVersion.ES6);
        assertEquals(TemplateType.V1, indexConfiguration.getTemplateType());
        assertEquals(IndexType.CUSTOM, indexConfiguration.getIndexType());
    }

    @Test
    public void testReadIndexConfig_documentRootKey() {
        final Map<String, Object> metadata = initializeConfigMetaData(
            IndexType.CUSTOM.getValue(), "foo", null, null, null, null);
        final String expectedRootKey = UUID.randomUUID().toString();
        metadata.put(DOCUMENT_ROOT_KEY, expectedRootKey);
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
        assertEquals(expectedRootKey, indexConfiguration.getDocumentRootKey());
    }

    @Test
    public void testReadIndexConfig_emptyDocumentRootKey() {
        final Map<String, Object> metadata = initializeConfigMetaData(
            IndexType.CUSTOM.getValue(), "foo", null, null, null, null);
        metadata.put(DOCUMENT_ROOT_KEY, "");
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        assertThrows(IllegalArgumentException.class, () -> IndexConfiguration.readIndexConfig(pluginSetting));
    }

    @Test
    void getTemplateType_defaults_to_V1() {
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.CUSTOM.getValue(), "foo", null, null, null, null);
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
        assertThat(indexConfiguration.getTemplateType(), equalTo(TemplateType.V1));
    }

    @ParameterizedTest
    @EnumSource(TemplateType.class)
    void getTemplateType_with_configured_templateType(final TemplateType templateType) {
        final Map<String, Object> metadata = initializeConfigMetaData(
                IndexType.CUSTOM.getValue(), "foo", null, null, null, null);
        metadata.put(TEMPLATE_TYPE, templateType.getTypeName());
        final PluginSetting pluginSetting = getPluginSetting(metadata);
        final IndexConfiguration indexConfiguration = IndexConfiguration.readIndexConfig(pluginSetting);
        assertThat(indexConfiguration.getTemplateType(), equalTo(templateType));
    }

    private PluginSetting generatePluginSetting(
            final String indexType, final String indexAlias, final String templateFilePath,
            final Long bulkSize, final Long flushTimeout, final String documentIdField) {
        final Map<String, Object> metadata = initializeConfigMetaData(indexType, indexAlias, templateFilePath, bulkSize, flushTimeout, documentIdField);
        return getPluginSetting(metadata);
    }

    private PluginSetting getPluginSetting(Map<String, Object> metadata) {
        return new PluginSetting("opensearch", metadata);
    }

    private Map<String, Object> initializeConfigMetaData(
            String indexType, String indexAlias, String templateFilePath, Long bulkSize, Long flushTimeout, String documentIdField) {
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
        if (bulkSize != null) {
            metadata.put(IndexConfiguration.BULK_SIZE, bulkSize);
        }
        if (flushTimeout != null) {
            metadata.put(IndexConfiguration.FLUSH_TIMEOUT, flushTimeout);
        }
        if (documentIdField != null) {
            metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, documentIdField);
        }
        return metadata;
    }
}
