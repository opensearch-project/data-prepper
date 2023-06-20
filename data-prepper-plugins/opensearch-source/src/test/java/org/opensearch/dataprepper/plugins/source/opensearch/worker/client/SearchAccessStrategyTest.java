/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchVersionInfo;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SearchConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessorStrategy.OPENSEARCH_DISTRIBUTION;

@ExtendWith(MockitoExtension.class)
public class SearchAccessStrategyTest {

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private ConnectionConfiguration connectionConfiguration;

    @BeforeEach
    void setup() {
        when(openSearchSourceConfiguration.getHosts()).thenReturn(List.of("http://localhost:9200"));
        when(openSearchSourceConfiguration.getConnectionConfiguration()).thenReturn(connectionConfiguration);
    }

    private SearchAccessorStrategy createObjectUnderTest() {
        return SearchAccessorStrategy.create(openSearchSourceConfiguration, awsCredentialsSupplier);
    }

    @ParameterizedTest
    @ValueSource(strings = {"2.5.0", "2.6.1", "3.0.0"})
    void testHappyPath_with_username_and_password_and_insecure_for_different_point_in_time_versions_for_opensearch(final String osVersion) {
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(username);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(password);

        when(connectionConfiguration.getCertPath()).thenReturn(null);
        when(connectionConfiguration.getSocketTimeout()).thenReturn(null);
        when(connectionConfiguration.getConnectTimeout()).thenReturn(null);
        when(connectionConfiguration.isInsecure()).thenReturn(true);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(null);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        when(openSearchSourceConfiguration.getAwsAuthenticationOptions()).thenReturn(null);

        final InfoResponse infoResponse = mock(InfoResponse.class);
        final OpenSearchVersionInfo openSearchVersionInfo = mock(OpenSearchVersionInfo.class);
        when(openSearchVersionInfo.distribution()).thenReturn(OPENSEARCH_DISTRIBUTION);
        when(openSearchVersionInfo.number()).thenReturn(osVersion);
        when(infoResponse.version()).thenReturn(openSearchVersionInfo);

        try (MockedConstruction<OpenSearchClient> openSearchClientMockedConstruction = mockConstruction(OpenSearchClient.class,
                (clientMock, context) -> {
                    when(clientMock.info()).thenReturn(infoResponse);
                })) {

            final SearchAccessor searchAccessor = createObjectUnderTest().getSearchAccessor();
            assertThat(searchAccessor, notNullValue());
            assertThat(searchAccessor.getSearchContextType(), equalTo(SearchContextType.POINT_IN_TIME));

            final List<OpenSearchClient> constructedClients = openSearchClientMockedConstruction.constructed();
            assertThat(constructedClients.size(), equalTo(1));
        }

        verifyNoInteractions(awsCredentialsSupplier);

    }

    @ParameterizedTest
    @ValueSource(strings = {"1.3.0", "2.4.9", "0.3.2"})
    void testHappyPath_with_aws_credentials_for_different_scroll_versions_for_opensearch(final String osVersion) {
        when(connectionConfiguration.getCertPath()).thenReturn(null);
        when(connectionConfiguration.getSocketTimeout()).thenReturn(null);
        when(connectionConfiguration.getConnectTimeout()).thenReturn(null);

        final AwsAuthenticationConfiguration awsAuthenticationConfiguration = mock(AwsAuthenticationConfiguration.class);
        when(awsAuthenticationConfiguration.getAwsRegion()).thenReturn(Region.US_EAST_1);
        final String stsRoleArn = "arn:aws:iam::123456789012:role/my-role";
        when(awsAuthenticationConfiguration.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsAuthenticationConfiguration.getAwsStsHeaderOverrides()).thenReturn(Collections.emptyMap());
        when(openSearchSourceConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationConfiguration);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(null);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final InfoResponse infoResponse = mock(InfoResponse.class);
        final OpenSearchVersionInfo openSearchVersionInfo = mock(OpenSearchVersionInfo.class);
        when(openSearchVersionInfo.distribution()).thenReturn(OPENSEARCH_DISTRIBUTION);
        when(openSearchVersionInfo.number()).thenReturn(osVersion);
        when(infoResponse.version()).thenReturn(openSearchVersionInfo);

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(awsCredentialsOptionsArgumentCaptor.capture())).thenReturn(awsCredentialsProvider);

        try (MockedConstruction<OpenSearchClient> openSearchClientMockedConstruction = mockConstruction(OpenSearchClient.class,
                (clientMock, context) -> {
                    when(clientMock.info()).thenReturn(infoResponse);
                })) {

            final SearchAccessor searchAccessor = createObjectUnderTest().getSearchAccessor();
            assertThat(searchAccessor, notNullValue());
            assertThat(searchAccessor.getSearchContextType(), equalTo(SearchContextType.SCROLL));

            final List<OpenSearchClient> constructedClients = openSearchClientMockedConstruction.constructed();
            assertThat(constructedClients.size(), equalTo(1));
        }

        final AwsCredentialsOptions awsCredentialsOptions = awsCredentialsOptionsArgumentCaptor.getValue();
        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), equalTo(Region.US_EAST_1));
        assertThat(awsCredentialsOptions.getStsHeaderOverrides(), equalTo(Collections.emptyMap()));
        assertThat(awsCredentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.3.0", "2.4.9", "0.3.2"})
    void search_context_type_set_to_point_in_time_with_invalid_version_throws_IllegalArgumentException(final String osVersion) {
        when(connectionConfiguration.getCertPath()).thenReturn(null);
        when(connectionConfiguration.getSocketTimeout()).thenReturn(null);
        when(connectionConfiguration.getConnectTimeout()).thenReturn(null);

        final AwsAuthenticationConfiguration awsAuthenticationConfiguration = mock(AwsAuthenticationConfiguration.class);
        when(awsAuthenticationConfiguration.getAwsRegion()).thenReturn(Region.US_EAST_1);
        final String stsRoleArn = "arn:aws:iam::123456789012:role/my-role";
        when(awsAuthenticationConfiguration.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsAuthenticationConfiguration.getAwsStsHeaderOverrides()).thenReturn(Collections.emptyMap());
        when(openSearchSourceConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationConfiguration);

        final InfoResponse infoResponse = mock(InfoResponse.class);
        final OpenSearchVersionInfo openSearchVersionInfo = mock(OpenSearchVersionInfo.class);
        when(openSearchVersionInfo.distribution()).thenReturn(OPENSEARCH_DISTRIBUTION);
        when(openSearchVersionInfo.number()).thenReturn(osVersion);
        when(infoResponse.version()).thenReturn(openSearchVersionInfo);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(SearchContextType.POINT_IN_TIME);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(awsCredentialsOptionsArgumentCaptor.capture())).thenReturn(awsCredentialsProvider);

        try (MockedConstruction<OpenSearchClient> openSearchClientMockedConstruction = mockConstruction(OpenSearchClient.class,
                (clientMock, context) -> {
                    when(clientMock.info()).thenReturn(infoResponse);
                })) {

            assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest().getSearchAccessor());

            final List<OpenSearchClient> constructedClients = openSearchClientMockedConstruction.constructed();
            assertThat(constructedClients.size(), equalTo(1));
        }

        final AwsCredentialsOptions awsCredentialsOptions = awsCredentialsOptionsArgumentCaptor.getValue();
        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), equalTo(Region.US_EAST_1));
        assertThat(awsCredentialsOptions.getStsHeaderOverrides(), equalTo(Collections.emptyMap()));
        assertThat(awsCredentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.3.0", "2.4.9", "2.5.0"})
    void search_context_type_set_to_none_uses_that_search_context_regardless_of_version(final String osVersion) {
        when(connectionConfiguration.getCertPath()).thenReturn(null);
        when(connectionConfiguration.getSocketTimeout()).thenReturn(null);
        when(connectionConfiguration.getConnectTimeout()).thenReturn(null);

        final AwsAuthenticationConfiguration awsAuthenticationConfiguration = mock(AwsAuthenticationConfiguration.class);
        when(awsAuthenticationConfiguration.getAwsRegion()).thenReturn(Region.US_EAST_1);
        final String stsRoleArn = "arn:aws:iam::123456789012:role/my-role";
        when(awsAuthenticationConfiguration.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsAuthenticationConfiguration.getAwsStsHeaderOverrides()).thenReturn(Collections.emptyMap());
        when(openSearchSourceConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationConfiguration);

        final InfoResponse infoResponse = mock(InfoResponse.class);
        final OpenSearchVersionInfo openSearchVersionInfo = mock(OpenSearchVersionInfo.class);
        when(openSearchVersionInfo.distribution()).thenReturn(OPENSEARCH_DISTRIBUTION);
        when(openSearchVersionInfo.number()).thenReturn(osVersion);
        when(infoResponse.version()).thenReturn(openSearchVersionInfo);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(SearchContextType.NONE);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(awsCredentialsOptionsArgumentCaptor.capture())).thenReturn(awsCredentialsProvider);

        try (MockedConstruction<OpenSearchClient> openSearchClientMockedConstruction = mockConstruction(OpenSearchClient.class,
                (clientMock, context) -> {
                    when(clientMock.info()).thenReturn(infoResponse);
                })) {

            final SearchAccessor searchAccessor = createObjectUnderTest().getSearchAccessor();
            assertThat(searchAccessor, notNullValue());
            assertThat(searchAccessor.getSearchContextType(), equalTo(SearchContextType.NONE));

            final List<OpenSearchClient> constructedClients = openSearchClientMockedConstruction.constructed();
            assertThat(constructedClients.size(), equalTo(1));
        }

        final AwsCredentialsOptions awsCredentialsOptions = awsCredentialsOptionsArgumentCaptor.getValue();
        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), equalTo(Region.US_EAST_1));
        assertThat(awsCredentialsOptions.getStsHeaderOverrides(), equalTo(Collections.emptyMap()));
        assertThat(awsCredentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
    }
}
