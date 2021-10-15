package com.amazon.dataprepper.plugins.sink.opensearch.index;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

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
    private final static String POLICY_NAME = "test-policy-name";

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    IndicesClient indicesClient;

    @Mock
    private RestClient restClient;

    @Mock
    private ResponseException responseException;

    @Before
    public void setup() {
        initMocks(this);
        ismPolicyManagementStrategy = new IsmPolicyManagement(restHighLevelClient,
                POLICY_NAME,
                IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE,
                IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE);

    }

    @Test
    public void constructor_NullRestClient() {
        assertThrows(NullPointerException.class, () ->
                new IsmPolicyManagement(null, POLICY_NAME, IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE, IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE));
        assertThrows(IllegalArgumentException.class, () ->
                new IsmPolicyManagement(restHighLevelClient, null, IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE, IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE));
        assertThrows(IllegalArgumentException.class, () ->
                new IsmPolicyManagement(restHighLevelClient, POLICY_NAME, null, IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE));
        assertThrows(IllegalArgumentException.class, () ->
                new IsmPolicyManagement(restHighLevelClient, POLICY_NAME, IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE, null));
    }

    @Test
    public void checkAndCreatePolicy_Normal() throws IOException {
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        assertEquals(Optional.empty(), ismPolicyManagementStrategy.checkAndCreatePolicy());
        verify(restHighLevelClient).getLowLevelClient();
        verify(restClient).performRequest(any());
    }

    @Test
    public void checkAndCreatePolicy_OnlyOnePolicyFile_TwoExceptions() throws IOException {
        ismPolicyManagementStrategy = new IsmPolicyManagement(restHighLevelClient,
                POLICY_NAME,
                IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE);
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        assertThrows(ResponseException.class, () -> ismPolicyManagementStrategy.checkAndCreatePolicy());
        verify(restHighLevelClient, times(2)).getLowLevelClient();
        verify(restClient, times(2)).performRequest(any());
    }

    @Test
    public void checkAndCreatePolicy_OnlyOnePolicyFile_FirstExceptionThenSucceeds() throws IOException {
        ismPolicyManagementStrategy = new IsmPolicyManagement(restHighLevelClient,
                POLICY_NAME,
                IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE);
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException).thenReturn(null);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        assertEquals(Optional.of(POLICY_NAME), ismPolicyManagementStrategy.checkAndCreatePolicy());
        verify(restHighLevelClient, times(2)).getLowLevelClient();
        verify(restClient, times(2)).performRequest(any());
    }

    @Test
    public void checkAndCreatePolicy_ExceptionFirstThenSucceed() throws IOException {
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException).thenReturn(null);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        assertEquals(Optional.of(POLICY_NAME), ismPolicyManagementStrategy.checkAndCreatePolicy());
        verify(restHighLevelClient, times(2)).getLowLevelClient();
        verify(restClient, times(2)).performRequest(any());
    }

    @Test
    public void getIndexPatterns() {
        assertEquals(Collections.singletonList(INDEX_ALIAS + "-*"), ismPolicyManagementStrategy.getIndexPatterns(INDEX_ALIAS));
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
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.existsAlias(any(GetAliasesRequest.class), any())).thenReturn(false);
        assertEquals(false, ismPolicyManagementStrategy.checkIfIndexExistsOnServer(INDEX_ALIAS));
    }

    @Test
    public void checkIfIndexExistsOnServer_true() throws IOException {
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        when(indicesClient.existsAlias(any(GetAliasesRequest.class), any())).thenReturn(true);
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
