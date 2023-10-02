package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchClientFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ElasticsearchClientRefresherTest {
    private static final String TEST_USERNAME = "test_user";
    private static final String TEST_PASSWORD = "test_password";

    @Mock
    private OpenSearchClientFactory openSearchClientFactory;

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private ElasticsearchClient elasticsearchClient;

    private ElasticsearchClientRefresher objectUnderTest;

    @BeforeEach
    void setup() {
        when(openSearchClientFactory.provideElasticSearchClient(eq(openSearchSourceConfiguration)))
                .thenReturn(elasticsearchClient);
        objectUnderTest = new ElasticsearchClientRefresher(openSearchClientFactory, openSearchSourceConfiguration);
    }

    @Test
    void testGet() {
        assertThat(objectUnderTest.get(), equalTo(elasticsearchClient));
    }

    @Test
    void testGetAfterUpdateWithBasicAuthUnchanged() {
        when(openSearchSourceConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(TEST_PASSWORD);
        final OpenSearchSourceConfiguration newConfig = mock(OpenSearchSourceConfiguration.class);
        when(newConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newConfig.getPassword()).thenReturn(TEST_PASSWORD);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(elasticsearchClient));
    }

    @Test
    void testGetAfterUpdateWithUsernameChanged() {
        when(openSearchSourceConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        final OpenSearchSourceConfiguration newConfig = mock(OpenSearchSourceConfiguration.class);
        when(newConfig.getUsername()).thenReturn(TEST_USERNAME + "_changed");
        final ElasticsearchClient newClient = mock(ElasticsearchClient.class);
        when(openSearchClientFactory.provideElasticSearchClient(eq(newConfig)))
                .thenReturn(newClient);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(newClient));
    }

    @Test
    void testGetAfterUpdateWithPasswordChanged() {
        when(openSearchSourceConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(TEST_PASSWORD);
        final OpenSearchSourceConfiguration newConfig = mock(OpenSearchSourceConfiguration.class);
        when(newConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newConfig.getPassword()).thenReturn(TEST_PASSWORD + "_changed");
        final ElasticsearchClient newClient = mock(ElasticsearchClient.class);
        when(openSearchClientFactory.provideElasticSearchClient(eq(newConfig)))
                .thenReturn(newClient);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(newClient));
    }
}