/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import software.amazon.awssdk.regions.Region;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SemanticEnrichmentIndexCreatorTest {

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private ConnectionConfiguration connectionConfiguration;

    @Nested
    class ConstructorTests {

        @Test
        void test_constructor_serverless_usesProvidedResourceName() {
            final String resourceName = UUID.randomUUID().toString();
            final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                    .withRegion(Region.US_WEST_2)
                    .build();

            when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
            when(connectionConfiguration.isServerless()).thenReturn(true);
            when(connectionConfiguration.getHosts()).thenReturn(List.of("https://dummy.us-west-2.aoss.amazonaws.com"));

            final SemanticEnrichmentIndexCreator creator =
                    new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, resourceName);

            assertThat(creator, notNullValue());
        }

        @Test
        void test_constructor_serverless_extractsFromHost_whenResourceNameNull() {
            final String collectionId = "abc123def456";
            final List<String> hosts = List.of("https://" + collectionId + ".us-west-2.aoss.amazonaws.com");
            final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                    .withRegion(Region.US_WEST_2)
                    .build();

            when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
            when(connectionConfiguration.isServerless()).thenReturn(true);
            when(connectionConfiguration.getHosts()).thenReturn(hosts);

            final SemanticEnrichmentIndexCreator creator =
                    new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, null);

            assertThat(creator, notNullValue());
        }

        @Test
        void test_constructor_serverless_extractsFromHost_whenResourceNameEmpty() {
            final String collectionId = "abc123def456";
            final List<String> hosts = List.of("https://" + collectionId + ".us-west-2.aoss.amazonaws.com");
            final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                    .withRegion(Region.US_WEST_2)
                    .build();

            when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
            when(connectionConfiguration.isServerless()).thenReturn(true);
            when(connectionConfiguration.getHosts()).thenReturn(hosts);

            final SemanticEnrichmentIndexCreator creator =
                    new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, "");

            assertThat(creator, notNullValue());
        }

        @Test
        void test_constructor_managedDomain_usesProvidedResourceName() {
            final String resourceName = UUID.randomUUID().toString();
            final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                    .withRegion(Region.US_EAST_1)
                    .build();

            when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
            when(connectionConfiguration.isServerless()).thenReturn(false);
            when(connectionConfiguration.getHosts()).thenReturn(List.of("https://search-test-abc.us-east-1.es.amazonaws.com"));

            final SemanticEnrichmentIndexCreator creator =
                    new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, resourceName);

            assertThat(creator, notNullValue());
        }

        @Test
        void test_constructor_managedDomain_extractsFromHost_whenResourceNameNull() {
            final List<String> hosts = List.of("https://search-mydomain-abc123.us-east-1.es.amazonaws.com");
            final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                    .withRegion(Region.US_EAST_1)
                    .build();

            when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
            when(connectionConfiguration.isServerless()).thenReturn(false);
            when(connectionConfiguration.getHosts()).thenReturn(hosts);

            final SemanticEnrichmentIndexCreator creator =
                    new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, null);

            assertThat(creator, notNullValue());
        }
    }

    @Nested
    class BuildIndexSchemaTests {

        @Test
        void test_buildIndexSchema_singleField_english_createsCorrectMapping() {
            final String fieldName = UUID.randomUUID().toString();
            final SemanticEnrichmentConfig config = mock(SemanticEnrichmentConfig.class);
            when(config.getFields()).thenReturn(List.of(Map.of(fieldName, SemanticEnrichmentLanguage.ENGLISH)));

            final Map<String, Object> result = createDefaultCreator().buildIndexSchema(config);

            assertThat(result, hasKey("mappings"));
            @SuppressWarnings("unchecked")
            final Map<String, Object> mappings = (Map<String, Object>) result.get("mappings");
            assertThat(mappings, hasKey("properties"));
            @SuppressWarnings("unchecked")
            final Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            assertThat(properties, hasKey(fieldName));
            @SuppressWarnings("unchecked")
            final Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(fieldName);
            assertThat(fieldMapping.get("type"), equalTo("text"));
            assertThat(fieldMapping, hasKey("semantic_enrichment"));
            @SuppressWarnings("unchecked")
            final Map<String, String> semanticEnrichment = (Map<String, String>) fieldMapping.get("semantic_enrichment");
            assertThat(semanticEnrichment.get("status"), equalTo("ENABLED"));
            assertThat(semanticEnrichment.get("language_options"), equalTo("english"));
        }

        @Test
        void test_buildIndexSchema_singleField_multilingual_createsCorrectMapping() {
            final String fieldName = UUID.randomUUID().toString();
            final SemanticEnrichmentConfig config = mock(SemanticEnrichmentConfig.class);
            when(config.getFields()).thenReturn(List.of(Map.of(fieldName, SemanticEnrichmentLanguage.MULTILINGUAL)));

            final Map<String, Object> result = createDefaultCreator().buildIndexSchema(config);

            @SuppressWarnings("unchecked")
            final Map<String, Object> mappings = (Map<String, Object>) result.get("mappings");
            @SuppressWarnings("unchecked")
            final Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            @SuppressWarnings("unchecked")
            final Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(fieldName);
            @SuppressWarnings("unchecked")
            final Map<String, String> semanticEnrichment = (Map<String, String>) fieldMapping.get("semantic_enrichment");
            assertThat(semanticEnrichment.get("language_options"), equalTo("multilingual"));
        }

        @Test
        void test_buildIndexSchema_multipleFields_differentLanguages_createsAllMappings() {
            final String field1 = UUID.randomUUID().toString();
            final String field2 = UUID.randomUUID().toString();
            final SemanticEnrichmentConfig config = mock(SemanticEnrichmentConfig.class);
            when(config.getFields()).thenReturn(List.of(
                    Map.of(field1, SemanticEnrichmentLanguage.ENGLISH),
                    Map.of(field2, SemanticEnrichmentLanguage.MULTILINGUAL)));

            final Map<String, Object> result = createDefaultCreator().buildIndexSchema(config);

            @SuppressWarnings("unchecked")
            final Map<String, Object> mappings = (Map<String, Object>) result.get("mappings");
            @SuppressWarnings("unchecked")
            final Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            assertThat(properties, hasKey(field1));
            assertThat(properties, hasKey(field2));

            @SuppressWarnings("unchecked")
            final Map<String, Object> field1Mapping = (Map<String, Object>) properties.get(field1);
            @SuppressWarnings("unchecked")
            final Map<String, String> field1Enrichment = (Map<String, String>) field1Mapping.get("semantic_enrichment");
            assertThat(field1Enrichment.get("language_options"), equalTo("english"));

            @SuppressWarnings("unchecked")
            final Map<String, Object> field2Mapping = (Map<String, Object>) properties.get(field2);
            @SuppressWarnings("unchecked")
            final Map<String, String> field2Enrichment = (Map<String, String>) field2Mapping.get("semantic_enrichment");
            assertThat(field2Enrichment.get("language_options"), equalTo("multilingual"));
        }
    }

    private SemanticEnrichmentIndexCreator createDefaultCreator() {
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .withRegion(Region.US_WEST_2)
                .build();

        when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
        when(connectionConfiguration.isServerless()).thenReturn(false);
        when(connectionConfiguration.getHosts()).thenReturn(List.of("https://search-testdomain-abc123.us-west-2.es.amazonaws.com"));

        return new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, "testdomain");
    }
}
