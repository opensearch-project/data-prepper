/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SemanticEnrichmentIndexCreatorTest {

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private ConnectionConfiguration connectionConfiguration;

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    @Nested
    class ExtractCollectionIdTests {

        @Test
        void extractCollectionId_returnsFirstSegmentOfHostname() {
            final String collectionId = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            final List<String> hosts = List.of("https://" + collectionId + ".us-west-2.aoss.amazonaws.com");

            final String result = SemanticEnrichmentIndexCreator.extractCollectionId(hosts);

            assertThat(result, equalTo(collectionId));
        }

        @Test
        void extractCollectionId_multipleHosts_usesFirstHost() {
            final String collectionId = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            final List<String> hosts = List.of(
                    "https://" + collectionId + ".us-east-1.aoss.amazonaws.com",
                    "https://other.us-east-1.aoss.amazonaws.com"
            );

            final String result = SemanticEnrichmentIndexCreator.extractCollectionId(hosts);

            assertThat(result, equalTo(collectionId));
        }

        @Test
        void extractCollectionId_nullHosts_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class,
                    () -> SemanticEnrichmentIndexCreator.extractCollectionId(null));
        }

        @Test
        void extractCollectionId_emptyHosts_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class,
                    () -> SemanticEnrichmentIndexCreator.extractCollectionId(Collections.emptyList()));
        }
    }

    @Nested
    class ExtractDomainNameTests {

        @Test
        void extractDomainName_validSearchPrefix_returnsDomainName() {
            final List<String> hosts = List.of("https://search-mydomain-abc123def.us-west-2.es.amazonaws.com");

            final String result = SemanticEnrichmentIndexCreator.extractDomainName(hosts);

            assertThat(result, equalTo("mydomain"));
        }

        @Test
        void extractDomainName_validVpcPrefix_returnsDomainName() {
            final List<String> hosts = List.of("https://vpc-mydomain-abc123def.us-west-2.es.amazonaws.com");

            final String result = SemanticEnrichmentIndexCreator.extractDomainName(hosts);

            assertThat(result, equalTo("mydomain"));
        }

        @Test
        void extractDomainName_domainWithHyphens_returnsDomainName() {
            final List<String> hosts = List.of("https://search-my-cool-domain-abc123def.us-west-2.es.amazonaws.com");

            final String result = SemanticEnrichmentIndexCreator.extractDomainName(hosts);

            assertThat(result, equalTo("my-cool-domain"));
        }

        @Test
        void extractDomainName_multipleHosts_usesFirstHost() {
            final List<String> hosts = List.of(
                    "https://search-firstdomain-abc123.us-west-2.es.amazonaws.com",
                    "https://search-seconddomain-def456.us-west-2.es.amazonaws.com"
            );

            final String result = SemanticEnrichmentIndexCreator.extractDomainName(hosts);

            assertThat(result, equalTo("firstdomain"));
        }

        @Test
        void extractDomainName_nullHosts_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class,
                    () -> SemanticEnrichmentIndexCreator.extractDomainName(null));
        }

        @Test
        void extractDomainName_emptyHosts_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class,
                    () -> SemanticEnrichmentIndexCreator.extractDomainName(Collections.emptyList()));
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "https://search-x.us-west-2.es.amazonaws.com",
                "https://vpc-x.us-west-2.es.amazonaws.com"
        })
        void extractDomainName_noHyphenInDomainPart_throwsIllegalArgumentException(final String host) {
            final List<String> hosts = List.of(host);

            assertThrows(IllegalArgumentException.class,
                    () -> SemanticEnrichmentIndexCreator.extractDomainName(hosts));
        }
    }

    @Nested
    class ConstructorTests {

        @Test
        void constructor_serverlessTrue_extractsCollectionId() {
            final String collectionId = "abc123def456";
            final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                    .withRegion(Region.US_WEST_2)
                    .build();
            final SemanticEnrichmentConfig semanticConfig = mock(SemanticEnrichmentConfig.class);

            when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
            when(connectionConfiguration.isServerless()).thenReturn(true);
            when(connectionConfiguration.getHosts()).thenReturn(
                    List.of("https://" + collectionId + ".us-west-2.aoss.amazonaws.com"));

            final SemanticEnrichmentIndexCreator creator =
                    new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, semanticConfig);

            assertThat(creator, notNullValue());
        }

        @Test
        void constructor_serverless_usesConfiguredCollectionName() {
            final String collectionName = UUID.randomUUID().toString();
            final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                    .withRegion(Region.US_WEST_2)
                    .build();
            final SemanticEnrichmentConfig semanticConfig = mock(SemanticEnrichmentConfig.class);
            when(semanticConfig.getCollectionName()).thenReturn(collectionName);

            when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
            when(connectionConfiguration.isServerless()).thenReturn(true);

            final SemanticEnrichmentIndexCreator creator =
                    new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, semanticConfig);

            assertThat(creator, notNullValue());
        }

        @Test
        void constructor_serverless_fallsBackToHostnameExtraction_whenCollectionNameEmpty() {
            final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                    .withRegion(Region.US_WEST_2)
                    .build();
            final SemanticEnrichmentConfig semanticConfig = mock(SemanticEnrichmentConfig.class);
            when(semanticConfig.getCollectionName()).thenReturn("");

            when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
            when(connectionConfiguration.isServerless()).thenReturn(true);
            when(connectionConfiguration.getHosts()).thenReturn(
                    List.of("https://abc123def456.us-west-2.aoss.amazonaws.com"));

            final SemanticEnrichmentIndexCreator creator =
                    new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, semanticConfig);

            assertThat(creator, notNullValue());
        }

        @Test
        void constructor_managedDomain_usesConfiguredDomainName() {
            final String domainName = UUID.randomUUID().toString();
            final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                    .withRegion(Region.US_EAST_1)
                    .build();
            final SemanticEnrichmentConfig semanticConfig = mock(SemanticEnrichmentConfig.class);
            when(semanticConfig.getDomainName()).thenReturn(domainName);

            when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
            when(connectionConfiguration.isServerless()).thenReturn(false);

            final SemanticEnrichmentIndexCreator creator =
                    new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, semanticConfig);

            assertThat(creator, notNullValue());
        }

        @Test
        void constructor_managedDomain_fallsBackToHostnameExtraction_whenDomainNameNotConfigured() {
            final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                    .withRegion(Region.US_EAST_1)
                    .build();
            final SemanticEnrichmentConfig semanticConfig = mock(SemanticEnrichmentConfig.class);
            when(semanticConfig.getDomainName()).thenReturn(null);

            when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
            when(connectionConfiguration.isServerless()).thenReturn(false);
            when(connectionConfiguration.getHosts()).thenReturn(
                    List.of("https://search-mydomain-abc123.us-east-1.es.amazonaws.com"));

            final SemanticEnrichmentIndexCreator creator =
                    new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, semanticConfig);

            assertThat(creator, notNullValue());
        }

        @Test
        void constructor_managedDomain_fallsBackToHostnameExtraction_whenDomainNameEmpty() {
            final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                    .withRegion(Region.US_WEST_2)
                    .build();
            final SemanticEnrichmentConfig semanticConfig = mock(SemanticEnrichmentConfig.class);
            when(semanticConfig.getDomainName()).thenReturn("");

            when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
            when(connectionConfiguration.isServerless()).thenReturn(false);
            when(connectionConfiguration.getHosts()).thenReturn(
                    List.of("https://search-testdomain-abc123.us-west-2.es.amazonaws.com"));

            final SemanticEnrichmentIndexCreator creator =
                    new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, semanticConfig);

            assertThat(creator, notNullValue());
        }
    }

    @Nested
    class BuildIndexSchemaTests {

        @Test
        void buildIndexSchema_singleField_createsCorrectMapping() {
            final String fieldName = UUID.randomUUID().toString();
            final SemanticEnrichmentConfig config = mock(SemanticEnrichmentConfig.class);
            when(config.getFields()).thenReturn(List.of(fieldName));
            when(config.getLanguage()).thenReturn("english");

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
        void buildIndexSchema_multipleFields_createsAllMappings() {
            final String field1 = UUID.randomUUID().toString();
            final String field2 = UUID.randomUUID().toString();
            final SemanticEnrichmentConfig config = mock(SemanticEnrichmentConfig.class);
            when(config.getFields()).thenReturn(List.of(field1, field2));
            when(config.getLanguage()).thenReturn("english");

            final Map<String, Object> result = createDefaultCreator().buildIndexSchema(config);

            @SuppressWarnings("unchecked")
            final Map<String, Object> mappings = (Map<String, Object>) result.get("mappings");
            @SuppressWarnings("unchecked")
            final Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            assertThat(properties, hasKey(field1));
            assertThat(properties, hasKey(field2));
        }

        @Test
        void buildIndexSchema_customLanguage_usesCustomLanguage() {
            final String fieldName = UUID.randomUUID().toString();
            final String language = UUID.randomUUID().toString();
            final SemanticEnrichmentConfig config = mock(SemanticEnrichmentConfig.class);
            when(config.getFields()).thenReturn(List.of(fieldName));
            when(config.getLanguage()).thenReturn(language);

            final Map<String, Object> result = createDefaultCreator().buildIndexSchema(config);

            @SuppressWarnings("unchecked")
            final Map<String, Object> mappings = (Map<String, Object>) result.get("mappings");
            @SuppressWarnings("unchecked")
            final Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            @SuppressWarnings("unchecked")
            final Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(fieldName);
            @SuppressWarnings("unchecked")
            final Map<String, String> semanticEnrichment = (Map<String, String>) fieldMapping.get("semantic_enrichment");
            assertThat(semanticEnrichment.get("language_options"), equalTo(language));
        }
    }


    private SemanticEnrichmentIndexCreator createDefaultCreator() {
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
                .withRegion(Region.US_WEST_2)
                .build();
        final SemanticEnrichmentConfig semanticConfig = mock(SemanticEnrichmentConfig.class);
        when(semanticConfig.getDomainName()).thenReturn("testdomain");

        when(connectionConfiguration.createAwsCredentialsOptions()).thenReturn(awsCredentialsOptions);
        when(connectionConfiguration.isServerless()).thenReturn(false);

        return new SemanticEnrichmentIndexCreator(awsCredentialsSupplier, connectionConfiguration, semanticConfig);
    }
}
