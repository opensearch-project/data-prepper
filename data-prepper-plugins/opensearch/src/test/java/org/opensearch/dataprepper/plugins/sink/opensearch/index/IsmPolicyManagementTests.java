/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.ExistsAliasRequest;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class IsmPolicyManagementTests {
    private IsmPolicyManagement ismPolicyManagementStrategy;
    private final String INDEX_ALIAS = "test-alias-abcd";
    private static final String POLICY_NAME = "test-policy-name";
    private final String TEST_ISM_FILE_PATH_S3 = "s3://folder/file.json";
    private final String TEST_ISM_FILE_PATH = "test-raw-span-policy-with-ism-template.json";

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    OpenSearchIndicesClient openSearchIndicesClient;

    @Mock
    private RestClient restClient;

    @Mock
    private ResponseException responseException;

    @Mock
    private S3Client s3Client;

    @BeforeEach
    public void setup() {
        initMocks(this);
        ismPolicyManagementStrategy = new IsmPolicyManagement(
                openSearchClient,
                restHighLevelClient,
                POLICY_NAME,
                IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE,
                IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE);

    }

    @Test
    public void constructor_NullRestClient() {
        assertThrows(NullPointerException.class, () ->
                new IsmPolicyManagement(
                        openSearchClient, null, POLICY_NAME, IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE, IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE));
        assertThrows(IllegalArgumentException.class, () ->
                new IsmPolicyManagement(
                        openSearchClient, restHighLevelClient, null, IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE, IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE));
        assertThrows(IllegalArgumentException.class, () ->
                new IsmPolicyManagement(
                        openSearchClient, restHighLevelClient, POLICY_NAME, null, IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE));
        assertThrows(IllegalArgumentException.class, () ->
                new IsmPolicyManagement(
                        openSearchClient, restHighLevelClient, POLICY_NAME, IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE, (String) null));
    }

    @Test
    public void checkAndCreatePolicy_Normal() throws IOException {
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        assertEquals(Optional.empty(), ismPolicyManagementStrategy.checkAndCreatePolicy(INDEX_ALIAS));
        verify(restHighLevelClient).getLowLevelClient();
        verify(restClient).performRequest(any());
    }

    @Test
    public void checkAndCreatePolicy_OnlyOnePolicyFile_TwoExceptions() throws IOException {
        ismPolicyManagementStrategy = new IsmPolicyManagement(
                openSearchClient,
                restHighLevelClient,
                POLICY_NAME,
                IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE, s3Client);
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        assertThrows(ResponseException.class, () -> ismPolicyManagementStrategy.checkAndCreatePolicy(INDEX_ALIAS));
        verify(restHighLevelClient, times(2)).getLowLevelClient();
        verify(restClient, times(2)).performRequest(any());
    }

    @Test
    public void checkAndCreatePolicy_OnlyOnePolicyFile_FirstExceptionThenSucceeds() throws IOException {
        ismPolicyManagementStrategy = new IsmPolicyManagement(
                openSearchClient,
                restHighLevelClient,
                POLICY_NAME,
                IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE, s3Client);
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException).thenReturn(null);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        assertEquals(Optional.of(POLICY_NAME), ismPolicyManagementStrategy.checkAndCreatePolicy(INDEX_ALIAS));
        verify(restHighLevelClient, times(2)).getLowLevelClient();
        verify(restClient, times(2)).performRequest(any());
    }

    @Test
    public void checkAndCreatePolicy_with_custom_ism_policy_from_s3() throws IOException {
        IsmPolicyManagement ismPolicyManagementStrategyWithTemplate = new IsmPolicyManagement(
                openSearchClient,
                restHighLevelClient,
                POLICY_NAME,
                TEST_ISM_FILE_PATH_S3, s3Client);

        final File certFilePath = new File(Objects.requireNonNull(IsmPolicyManagementTests.class.getClassLoader()
                .getResource(TEST_ISM_FILE_PATH)).getFile());
        final String fileContent = FileUtils.readFileToString(certFilePath, StandardCharsets.UTF_8);

        final InputStream fileObjectStream = IOUtils.toInputStream(fileContent, StandardCharsets.UTF_8);
        final ResponseInputStream<GetObjectResponse> fileInputStream = new ResponseInputStream<>(
                GetObjectResponse.builder().contentLength(ONE_MB).build(),
                AbortableInputStream.create(fileObjectStream)
        );

        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(fileInputStream);
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException).thenReturn(null);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        assertEquals(Optional.of(POLICY_NAME), ismPolicyManagementStrategyWithTemplate.checkAndCreatePolicy(INDEX_ALIAS));
        verify(restHighLevelClient, times(2)).getLowLevelClient();
        verify(restClient, times(2)).performRequest(any());
    }

    @Test
    public void checkAndCreatePolicy_ExceptionFirstThenSucceed() throws IOException {
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException).thenReturn(null);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        assertEquals(Optional.of(POLICY_NAME), ismPolicyManagementStrategy.checkAndCreatePolicy(INDEX_ALIAS));
        verify(restHighLevelClient, times(2)).getLowLevelClient();
        verify(restClient, times(2)).performRequest(any());
    }

    @ParameterizedTest
    @CsvSource({
            "test-index, test-index-*",
            "%{yyyy-MM}-test-index, *-test-index-*",
            "test-%{yyyy-MM}-index, test-*-index-*",
            "test-index-%{yyyy-MM}, test-index-*-*"
    })
    public void getIndexPatterns(final String indexAlias, final String expectedIndexPattern) {
        assertEquals(Collections.singletonList(expectedIndexPattern), ismPolicyManagementStrategy.getIndexPatterns(indexAlias));
    }

    @Test
    public void getIndexPatterns_NullInput_Exception() {
        assertThrows(IllegalArgumentException.class,
                () -> ismPolicyManagementStrategy.getIndexPatterns(null)
        );
    }

    @Test
    public void checkIfIndexExistsOnServer_NullInput_Exception() {
        assertThrows(IllegalArgumentException.class,
                () -> ismPolicyManagementStrategy.checkIfIndexExistsOnServer(null)
        );
    }

    @Test
    public void checkIfIndexExistsOnServer_false() throws IOException {
        when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(false));
        assertEquals(false, ismPolicyManagementStrategy.checkIfIndexExistsOnServer(INDEX_ALIAS));
    }

    @Test
    public void checkIfIndexExistsOnServer_true() throws IOException {
        when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(true));
        assertEquals(true, ismPolicyManagementStrategy.checkIfIndexExistsOnServer(INDEX_ALIAS));
    }

    @Test
    public void getCreateIndexRequest_NullInput_Exception() {
        assertThrows(IllegalArgumentException.class,
                () -> ismPolicyManagementStrategy.getCreateIndexRequest(null)
        );
    }

    @Test
    public void getCreateIndexRequest() {
        assertNotNull(ismPolicyManagementStrategy.getCreateIndexRequest(INDEX_ALIAS));
    }

}
