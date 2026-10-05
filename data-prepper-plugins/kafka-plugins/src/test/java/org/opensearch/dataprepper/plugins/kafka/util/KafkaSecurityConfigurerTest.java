package org.opensearch.dataprepper.plugins.kafka.util;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginConfigObserver;
import org.opensearch.dataprepper.plugins.kafka.authenticator.AwsCredentialsSupplierProvider;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicBasicCredentialsProvider;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicSaslClientCallbackHandler;
import org.opensearch.dataprepper.plugins.kafka.common.aws.AwsContext;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsCredentialsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AzureFederatedAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.KafkaClientBuilder;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersRequest;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersResponse;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.StsException;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.common.config.SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

@ExtendWith(MockitoExtension.class)
public class KafkaSecurityConfigurerTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSecurityConfigurerTest.class);

    @Mock
    private PluginConfigObservable pluginConfigObservable;
    @Mock
    private DynamicBasicCredentialsProvider dynamicBasicCredentialsProvider;
    @Mock
    private KafkaConnectionConfig kafkaConnectionConfig;
    @Mock
    private AuthConfig authConfig;
    @Mock
    private AuthConfig.SaslAuthConfig saslAuthConfig;
    @Mock
    private PlainTextAuthConfig plainTextAuthConfig;
    @Mock
    private AwsContext awsContext;
    @Mock
    private StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider;
    @Captor
    private ArgumentCaptor<PluginConfigObserver> pluginConfigObserverArgumentCaptor;

    @AfterEach
    void resetAwsCredentialsSupplierSingleton() {
        AwsCredentialsSupplierProvider.getInstance().set(null);
    }

    @Test
    public void testSetAuthPropertiesWithSaslPlainCertificate() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-sasl-ssl-certificate-content.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is("CERTIFICATE_DATA"));
        assertThat(props.getProperty("ssl.truststore.location"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.password"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(CustomClientSslEngineFactory.class));
    }

    @Test
    public void testSetAuthPropertiesWithNoAuthSsl() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-no-auth-ssl.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is(nullValue()));
        assertThat(props.getProperty("security.protocol"), is("SSL"));
        assertThat(props.getProperty("certificateContent"), is("CERTIFICATE_DATA"));
        assertThat(props.get("ssl.engine.factory.class"), is(CustomClientSslEngineFactory.class));
    }
    @Test
    public void testSetAuthPropertiesWithNoAuthSslNone() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-no-auth-ssl-none.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is(nullValue()));
        assertThat(props.getProperty("security.protocol"), is(nullValue()));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
    }

    @Test
    public void testSetAuthPropertiesWithNoAuthInsecure() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-auth-insecure.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_PLAINTEXT"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(InsecureSslEngineFactory.class));
    }
    @Test
    public void testSetAuthPropertiesAuthSslWithTrustStore() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-sasl-ssl-truststore.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.location"), is("some-file-path"));
        assertThat(props.getProperty("ssl.truststore.password"), is("some-password"));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
    }

    @Test
    public void testSetAuthPropertiesAuthSslWithNoCertContentNoTrustStore() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-sasl-ssl-no-cert-content-no-truststore.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.location"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.password"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
    }

    @Test
    public void testSetAuthPropertiesBootstrapServersWithSaslIAMRole() throws IOException {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-bootstrap-servers-sasl-iam-role.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
        assertThat(props.getProperty("bootstrap.servers"), is("localhost:9092"));
        assertThat(props.getProperty("sasl.mechanism"), is("AWS_MSK_IAM"));
        assertThat(props.getProperty("sasl.jaas.config"),
                is("software.amazon.msk.auth.iam.IAMLoginModule required " +
                        "awsRoleArn=\"test_sasl_iam_sts_role\" awsStsRegion=\"us-east-2\";"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.location"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.password"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
        assertThat(props.get("sasl.client.callback.handler.class"),
                is("software.amazon.msk.auth.iam.IAMClientCallbackHandler"));
    }

    @Test
    public void testSetAuthPropertiesBootstrapServersWithSaslIAMDefault() throws IOException {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-bootstrap-servers-sasl-iam-default.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
        assertThat(props.getProperty("bootstrap.servers"), is("localhost:9092"));
        assertThat(props.getProperty("sasl.jaas.config"), is("software.amazon.msk.auth.iam.IAMLoginModule required;"));
        assertThat(props.getProperty("sasl.mechanism"), is("AWS_MSK_IAM"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.location"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.password"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
        assertThat(props.get("sasl.client.callback.handler.class"),
                is("software.amazon.msk.auth.iam.IAMClientCallbackHandler"));
    }

    @Test
    public void testSetAuthPropertiesBootstrapServersOverrideByMSK() throws IOException {
        final String testMSKEndpoint = UUID.randomUUID().toString();
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-bootstrap-servers-override-by-msk.yaml");
        final KafkaClientBuilder kafkaClientBuilder = mock(KafkaClientBuilder.class);
        final KafkaClient kafkaClient = mock(KafkaClient.class);
        when(kafkaClientBuilder.credentialsProvider(any())).thenReturn(kafkaClientBuilder);
        when(kafkaClientBuilder.region(any(Region.class))).thenReturn(kafkaClientBuilder);
        when(kafkaClientBuilder.build()).thenReturn(kafkaClient);
        final GetBootstrapBrokersResponse response = mock(GetBootstrapBrokersResponse.class);
        when(response.bootstrapBrokerStringSaslIam()).thenReturn(testMSKEndpoint);
        when(kafkaClient.getBootstrapBrokers(any(GetBootstrapBrokersRequest.class))).thenReturn(response);
        try (MockedStatic<KafkaClient> mockedKafkaClient = mockStatic(KafkaClient.class)) {
            mockedKafkaClient.when(KafkaClient::builder).thenReturn(kafkaClientBuilder);
            KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
        }
        assertThat(props.getProperty("bootstrap.servers"), is(testMSKEndpoint));
        assertThat(props.getProperty("sasl.mechanism"), is("AWS_MSK_IAM"));
        assertThat(props.getProperty("sasl.jaas.config"),
                is("software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn=\"sts_role_arn\" awsStsRegion=\"us-east-2\";"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.location"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.password"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
        assertThat(props.get("sasl.client.callback.handler.class"),
                is("software.amazon.msk.auth.iam.IAMClientCallbackHandler"));
    }

    @Test
    public void testGetBootStrapServersForMsk_StsException403_ThrowsImmediately() throws IOException {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-bootstrap-servers-override-by-msk.yaml");
        final KafkaClientBuilder kafkaClientBuilder = mock(KafkaClientBuilder.class);
        final KafkaClient kafkaClient = mock(KafkaClient.class);
        when(kafkaClientBuilder.credentialsProvider(any())).thenReturn(kafkaClientBuilder);
        when(kafkaClientBuilder.region(any(Region.class))).thenReturn(kafkaClientBuilder);
        when(kafkaClientBuilder.build()).thenReturn(kafkaClient);

        final StsException stsException = (StsException) StsException.builder()
                .statusCode(403)
                .message("Access Denied")
                .build();

        when(kafkaClient.getBootstrapBrokers(any(GetBootstrapBrokersRequest.class)))
                .thenThrow(stsException);

        try (MockedStatic<KafkaClient> mockedKafkaClient = mockStatic(KafkaClient.class)) {
            mockedKafkaClient.when(KafkaClient::builder).thenReturn(kafkaClientBuilder);

            RuntimeException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                    RuntimeException.class,
                    () -> KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG)
            );

            assertThat(thrown.getMessage(), is("Access denied when calling STS to get bootstrap server information from MSK. " +
                    "Verify that the role exists and the trust policy is correctly configured."));

            verify(kafkaClient, times(1)).getBootstrapBrokers(any(GetBootstrapBrokersRequest.class));
        }
    }

    @Test
    public void testGetBootStrapServersForMsk_StsExceptionNon403_Retries() throws IOException {
        final String testMSKEndpoint = "test-endpoint:9098";
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-bootstrap-servers-override-by-msk.yaml");
        final KafkaClientBuilder kafkaClientBuilder = mock(KafkaClientBuilder.class);
        final KafkaClient kafkaClient = mock(KafkaClient.class);
        when(kafkaClientBuilder.credentialsProvider(any())).thenReturn(kafkaClientBuilder);
        when(kafkaClientBuilder.region(any(Region.class))).thenReturn(kafkaClientBuilder);
        when(kafkaClientBuilder.build()).thenReturn(kafkaClient);

        final StsException stsException = (StsException) StsException.builder()
                .statusCode(500)
                .message("Internal Server Error")
                .build();

        final GetBootstrapBrokersResponse response = mock(GetBootstrapBrokersResponse.class);
        when(response.bootstrapBrokerStringSaslIam()).thenReturn(testMSKEndpoint);

        when(kafkaClient.getBootstrapBrokers(any(GetBootstrapBrokersRequest.class)))
                .thenThrow(stsException)
                .thenReturn(response);

        try (MockedStatic<KafkaClient> mockedKafkaClient = mockStatic(KafkaClient.class)) {
            mockedKafkaClient.when(KafkaClient::builder).thenReturn(kafkaClientBuilder);
            KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
        }

        assertThat(props.getProperty("bootstrap.servers"), is(testMSKEndpoint));
        verify(kafkaClient, times(2)).getBootstrapBrokers(any(GetBootstrapBrokersRequest.class));
    }

    @Test
    public void testSetAuthPropertiesMskWithSaslPlain() throws IOException {
        final String testMSKEndpoint = UUID.randomUUID().toString();
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-msk-sasl-plain.yaml");
        final KafkaClientBuilder kafkaClientBuilder = mock(KafkaClientBuilder.class);
        final KafkaClient kafkaClient = mock(KafkaClient.class);
        when(kafkaClientBuilder.credentialsProvider(any())).thenReturn(kafkaClientBuilder);
        when(kafkaClientBuilder.region(any(Region.class))).thenReturn(kafkaClientBuilder);
        when(kafkaClientBuilder.build()).thenReturn(kafkaClient);
        final GetBootstrapBrokersResponse response = mock(GetBootstrapBrokersResponse.class);
        when(response.bootstrapBrokerStringSaslIam()).thenReturn(testMSKEndpoint);
        when(kafkaClient.getBootstrapBrokers(any(GetBootstrapBrokersRequest.class))).thenReturn(response);
        try (MockedStatic<KafkaClient> mockedKafkaClient = mockStatic(KafkaClient.class)) {
            mockedKafkaClient.when(KafkaClient::builder).thenReturn(kafkaClientBuilder);
            KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
        }
        assertThat(props.getProperty("bootstrap.servers"), is(testMSKEndpoint));
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("sasl.jaas.config"),
                is("org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"test_sasl_username\" password=\"test_sasl_password\";"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.location"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.password"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "kafka-pipeline-bootstrap-servers-glue-sts-assume-role.yaml",
            "kafka-pipeline-msk-default-glue-sts-assume-role.yaml"
    })
    void testGetGlueSerializerWithStsAssumeRoleCredentialsProvider(final String filename) throws IOException {
        when(awsContext.getOrDefault(any(AwsCredentialsConfig.class))).thenReturn(stsAssumeRoleCredentialsProvider);
        when(awsContext.getRegionOrDefault(any(AwsCredentialsConfig.class))).thenReturn(Region.US_EAST_1);
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig(filename);
        final GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = KafkaSecurityConfigurer
                .getGlueSerializer(kafkaSourceConfig, awsContext);
        assertThat(glueSchemaRegistryKafkaDeserializer, notNullValue());
        assertThat(glueSchemaRegistryKafkaDeserializer.getCredentialProvider(),
                instanceOf(StsAssumeRoleCredentialsProvider.class));
    }

    @Test
    void testGetGlueSerializerWithDefaultCredentialsProvider() throws IOException {
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig(
                "kafka-pipeline-bootstrap-servers-glue-default.yaml");
        final DefaultCredentialsProvider defaultCredentialsProvider = mock(DefaultCredentialsProvider.class);
        when(awsContext.getOrDefault(any())).thenReturn(defaultCredentialsProvider);
        when(awsContext.getRegionOrDefault(any())).thenReturn(Region.US_EAST_1);
        final GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = KafkaSecurityConfigurer
                .getGlueSerializer(kafkaSourceConfig, awsContext);
        assertThat(glueSchemaRegistryKafkaDeserializer, notNullValue());
        assertThat(glueSchemaRegistryKafkaDeserializer.getCredentialProvider(),
                instanceOf(DefaultCredentialsProvider.class));
        assertThat(glueSchemaRegistryKafkaDeserializer
                .getGlueSchemaRegistryDeserializationFacade()
                .getGlueSchemaRegistryConfiguration()
                .getEndPoint(), is(nullValue()));
    }

    @Test
    void testGetGlueSerializerWithDefaultCredentialsProviderAndOverrridenRegistryEndpoint() throws IOException {
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig(
                "kafka-pipeline-bootstrap-servers-glue-override-endpoint.yaml");
        final DefaultCredentialsProvider defaultCredentialsProvider = mock(DefaultCredentialsProvider.class);
        when(awsContext.getOrDefault(any())).thenReturn(defaultCredentialsProvider);
        when(awsContext.getRegionOrDefault(any())).thenReturn(Region.US_EAST_1);
        final GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = KafkaSecurityConfigurer
                .getGlueSerializer(kafkaSourceConfig, awsContext);
        assertThat(glueSchemaRegistryKafkaDeserializer, notNullValue());
        assertThat(glueSchemaRegistryKafkaDeserializer.getCredentialProvider(),
                instanceOf(DefaultCredentialsProvider.class));
        assertThat(glueSchemaRegistryKafkaDeserializer
                .getGlueSchemaRegistryDeserializationFacade()
                .getGlueSchemaRegistryConfiguration()
                .getEndPoint(), is("http://fake-glue-registry"));
    }

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNonNullPlainTextAuthConfig() {
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        final Properties properties = new Properties();
        try (final MockedStatic<DynamicBasicCredentialsProvider> dynamicBasicCredentialsProviderMockedStatic =
                     mockStatic(DynamicBasicCredentialsProvider.class)) {
            dynamicBasicCredentialsProviderMockedStatic.when(DynamicBasicCredentialsProvider::getInstance)
                    .thenReturn(dynamicBasicCredentialsProvider);
            KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                    properties, kafkaConnectionConfig, pluginConfigObservable);
        }
        assertThat(properties.get(SASL_CLIENT_CALLBACK_HANDLER_CLASS), equalTo(DynamicSaslClientCallbackHandler.class));
        verify(dynamicBasicCredentialsProvider).refresh(kafkaConnectionConfig);
        verify(pluginConfigObservable).addPluginConfigObserver(pluginConfigObserverArgumentCaptor.capture());
        final PluginConfigObserver pluginConfigObserver = pluginConfigObserverArgumentCaptor.getValue();
        final KafkaConnectionConfig newConfig = mock(KafkaConnectionConfig.class);
        pluginConfigObserver.update(newConfig);
        verify(dynamicBasicCredentialsProvider).refresh(newConfig);
    }

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNullPlainTextAuthConfig() {
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        final Properties properties = new Properties();
        KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                properties, kafkaConnectionConfig, pluginConfigObservable);
        assertThat(properties.isEmpty(), is(true));
        verifyNoInteractions(pluginConfigObservable);
    }

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNullSaslAuthConfig() {
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        final Properties properties = new Properties();
        KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                properties, kafkaConnectionConfig, pluginConfigObservable);
        assertThat(properties.isEmpty(), is(true));
        verifyNoInteractions(pluginConfigObservable);
    }

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNullAuthConfig() {
        final Properties properties = new Properties();
        KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                properties, kafkaConnectionConfig, pluginConfigObservable);
        assertThat(properties.isEmpty(), is(true));
        verifyNoInteractions(pluginConfigObservable);
    }

    @Test
    void testSetAuthPropertiesWithStsHeaderOverrides() throws IOException {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-bootstrap-servers-sasl-iam-role.yaml");
        
        try (MockedStatic<StsAssumeRoleCredentialsProvider> mockedProvider = mockStatic(StsAssumeRoleCredentialsProvider.class)) {
            final StsAssumeRoleCredentialsProvider.Builder mockBuilder = mock(StsAssumeRoleCredentialsProvider.Builder.class);
            when(mockBuilder.stsClient(any())).thenReturn(mockBuilder);
            when(mockBuilder.refreshRequest(any(AssumeRoleRequest.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(stsAssumeRoleCredentialsProvider);
            mockedProvider.when(StsAssumeRoleCredentialsProvider::builder).thenReturn(mockBuilder);
            
            KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);
            
            verify(mockBuilder).refreshRequest(any(AssumeRoleRequest.class));
        }
    }

    @Test
    void testSetAuthPropertiesWithStsHeaderOverridesConfigured() throws IOException {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-bootstrap-servers-sasl-iam-role-with-headers.yaml");
        
        try (MockedStatic<StsAssumeRoleCredentialsProvider> mockedProvider = mockStatic(StsAssumeRoleCredentialsProvider.class)) {
            final StsAssumeRoleCredentialsProvider.Builder stsCredentialsProviderBuilder = mock(StsAssumeRoleCredentialsProvider.Builder.class);
            when(stsCredentialsProviderBuilder.stsClient(any())).thenReturn(stsCredentialsProviderBuilder);
            when(stsCredentialsProviderBuilder.refreshRequest(any(AssumeRoleRequest.class))).thenReturn(stsCredentialsProviderBuilder);
            when(stsCredentialsProviderBuilder.build()).thenReturn(stsAssumeRoleCredentialsProvider);
            mockedProvider.when(StsAssumeRoleCredentialsProvider::builder).thenReturn(stsCredentialsProviderBuilder);
            
            KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);

            final ArgumentCaptor<AssumeRoleRequest> assumeRoleRequestArgumentCaptor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
            verify(stsCredentialsProviderBuilder).refreshRequest(assumeRoleRequestArgumentCaptor.capture());
            final AssumeRoleRequest actualAssumeRoleRequest = assumeRoleRequestArgumentCaptor.getValue();
            assertThat(actualAssumeRoleRequest.overrideConfiguration(), notNullValue());
            assertThat(actualAssumeRoleRequest.overrideConfiguration().isPresent(), equalTo(true));
            final AwsRequestOverrideConfiguration overrideConfiguration = actualAssumeRoleRequest.overrideConfiguration().get();
            assertThat(overrideConfiguration.headers(), notNullValue());
            assertThat(overrideConfiguration.headers().size(), equalTo(2));
            final String headerName1 = "X-Custom-Header";
            final String headerValue1 = "custom-value";
            final String headerName2 = "X-Another-Header";
            final String headerValue2 = "another-value";
            assertThat(overrideConfiguration.headers(), hasKey(headerName1));
            assertThat(overrideConfiguration.headers(), hasKey(headerName2));
            assertThat(overrideConfiguration.headers().get(headerName1), notNullValue());
            assertThat(overrideConfiguration.headers().get(headerName1).size(), equalTo(1));
            assertThat(overrideConfiguration.headers().get(headerName1), hasItem(headerValue1));
            assertThat(overrideConfiguration.headers().get(headerName2), notNullValue());
            assertThat(overrideConfiguration.headers().get(headerName2).size(), equalTo(1));
            assertThat(overrideConfiguration.headers().get(headerName2), hasItem(headerValue2));
        }
    }

    @Test
    void testSetAuthPropertiesWithAzureFederated() throws IOException {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-azure-federated.yaml");

        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);

        assertThat(props.getProperty("sasl.mechanism"), is("OAUTHBEARER"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("sasl.login.callback.handler.class"),
                is("org.opensearch.dataprepper.plugins.kafka.authenticator.AzureFederatedCallbackHandler"));
        final String jaasConfig = props.getProperty("sasl.jaas.config");
        assertThat(jaasConfig, notNullValue());
        assertThat(jaasConfig, containsString("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required"));
        assertThat(jaasConfig, containsString("azureFederatedRegion=\"us-east-1\""));
        assertThat(jaasConfig, containsString("azureFederatedStsRoleArn=\"arn:aws:iam::123456789012:role/eh-federation\""));
        assertThat(jaasConfig, containsString(
                "azureFederatedTokenEndpoint=\"https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/v2.0/token\""));
        assertThat(jaasConfig, containsString("azureFederatedClientId=\"11111111-1111-1111-1111-111111111111\""));
        assertThat(jaasConfig, containsString("azureFederatedScope=\"https://my-namespace.servicebus.windows.net/.default\""));
    }

    @Test
    void testSetAuthPropertiesWithAzureFederated_differentConfigsProduceDifferentJaasConfig() throws IOException {
        final Properties firstProps = new Properties();
        KafkaSecurityConfigurer.setAuthProperties(firstProps,
                createKafkaSinkConfig("kafka-pipeline-azure-federated.yaml"), null, LOG);
        final Properties secondProps = new Properties();
        KafkaSecurityConfigurer.setAuthProperties(secondProps,
                createKafkaSinkConfig("kafka-pipeline-azure-federated-other-tenant.yaml"), null, LOG);

        assertThat(firstProps.getProperty("sasl.jaas.config"),
                is(not(secondProps.getProperty("sasl.jaas.config"))));
    }

    @Test
    void testSetAuthPropertiesWithAzureFederated_nullAwsConfig_noDefaultRegion_throws() throws Exception {
        final KafkaSourceConfig kafkaSourceConfig = azureFederatedSourceConfig(null);
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getDefaultRegion()).thenReturn(Optional.empty());

        final Properties props = new Properties();
        final RuntimeException e = assertThrows(RuntimeException.class,
                () -> KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, supplier, LOG));
        assertThat(e.getMessage(),
                is("azure_federated requires a region in the pipeline aws config or data-prepper-config.yaml"));
    }

    @Test
    void testSetAuthPropertiesWithAzureFederated_missingRegion_noDefaultRegion_throws() throws Exception {
        final AwsConfig awsConfig = new AwsConfig();
        setField(AwsConfig.class, awsConfig, "stsRoleArn", "arn:aws:iam::123456789012:role/eh-federation");
        final KafkaSourceConfig kafkaSourceConfig = azureFederatedSourceConfig(awsConfig);
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getDefaultRegion()).thenReturn(Optional.empty());

        final Properties props = new Properties();
        final RuntimeException e = assertThrows(RuntimeException.class,
                () -> KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, supplier, LOG));
        assertThat(e.getMessage(),
                is("azure_federated requires a region in the pipeline aws config or data-prepper-config.yaml"));
    }

    @Test
    void testSetAuthPropertiesWithAzureFederated_regionFromDefaultRegion() throws Exception {
        final KafkaSourceConfig config = azureFederatedConfig(null, null,
                "11111111-1111-1111-1111-111111111111",
                "https://login.microsoftonline.com/t/oauth2/v2.0/token",
                "https://ns.servicebus.windows.net/.default");
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getDefaultRegion()).thenReturn(Optional.of(Region.US_WEST_2));

        final Properties props = new Properties();
        KafkaSecurityConfigurer.setAuthProperties(props, config, supplier, LOG);

        assertThat(props.getProperty("sasl.jaas.config"), containsString("azureFederatedRegion=\"us-west-2\""));
    }

    @Test
    void testSetAuthPropertiesWithAzureFederated_missingStsRoleArn_omitsRoleJaasOption() throws Exception {
        final KafkaSourceConfig kafkaSourceConfig = azureFederatedConfig("us-east-1", null,
                "11111111-1111-1111-1111-111111111111",
                "https://login.microsoftonline.com/t/oauth2/v2.0/token",
                "https://ns.servicebus.windows.net/.default");
        final Properties props = new Properties();

        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, null, LOG);

        final String jaas = props.getProperty("sasl.jaas.config");
        assertThat(jaas, containsString("OAuthBearerLoginModule"));
        assertThat(jaas, not(containsString("azureFederatedStsRoleArn")));
        assertThat(jaas, not(containsString("null")));
    }

    @Test
    void setAuthProperties_azureFederatedWithoutRole_omitsRoleJaasOption() throws Exception {
        final KafkaSourceConfig config = azureFederatedConfig("us-east-1", null,
                "11111111-1111-1111-1111-111111111111",
                "https://login.microsoftonline.com/t/oauth2/v2.0/token",
                "https://ns.servicebus.windows.net/.default");
        final Properties properties = new Properties();

        KafkaSecurityConfigurer.setAuthProperties(properties, config, mock(AwsCredentialsSupplier.class), LOG);

        final String jaas = properties.getProperty("sasl.jaas.config");
        assertThat(jaas, containsString("OAuthBearerLoginModule"));
        assertThat(jaas, not(containsString("azureFederatedStsRoleArn")));
        assertThat(jaas, not(containsString("null")));
    }

    @Test
    void setAuthProperties_azureFederatedWithRole_includesRoleJaasOption() throws Exception {
        final KafkaSourceConfig config = azureFederatedConfig("us-east-1",
                "arn:aws:iam::123456789012:role/eh-federation",
                "11111111-1111-1111-1111-111111111111",
                "https://login.microsoftonline.com/t/oauth2/v2.0/token",
                "https://ns.servicebus.windows.net/.default");
        final Properties properties = new Properties();

        KafkaSecurityConfigurer.setAuthProperties(properties, config, mock(AwsCredentialsSupplier.class), LOG);

        assertThat(properties.getProperty("sasl.jaas.config"),
                containsString("azureFederatedStsRoleArn=\"arn:aws:iam::123456789012:role/eh-federation\""));
    }

    @Test
    void setAuthProperties_populatesSupplierSingleton() throws Exception {
        final KafkaSourceConfig config = azureFederatedConfig("us-east-1", null,
                "11111111-1111-1111-1111-111111111111",
                "https://login.microsoftonline.com/t/oauth2/v2.0/token",
                "https://ns.servicebus.windows.net/.default");
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);

        KafkaSecurityConfigurer.setAuthProperties(new Properties(), config, supplier, LOG);

        assertThat(AwsCredentialsSupplierProvider.getInstance().getAwsCredentialsSupplier(),
                sameInstance(supplier));
    }

    @Test
    void setAuthProperties_withNullSupplier_doesNotThrow() throws Exception {
        final KafkaSourceConfig config = azureFederatedConfig("us-east-1", null,
                "11111111-1111-1111-1111-111111111111",
                "https://login.microsoftonline.com/t/oauth2/v2.0/token",
                "https://ns.servicebus.windows.net/.default");
        AwsCredentialsSupplierProvider.getInstance().set(null);

        KafkaSecurityConfigurer.setAuthProperties(new Properties(), config, null, LOG);
        assertThat(AwsCredentialsSupplierProvider.getInstance().getAwsCredentialsSupplier(), nullValue());
    }

    @Test
    void setAuthProperties_azureFederatedWithStsHeaderOverrides_encodesThemInJaas() throws Exception {
        final Map<String, String> stsHeaderOverrides = Map.of(
                "x-amz-source-arn", "arn:aws:osis:us-east-1:123456789012:pipeline/p",
                "x-amz-source-account", "quoted \"value\" with spaces");
        final KafkaSourceConfig config = azureFederatedConfig("us-east-1",
                "arn:aws:iam::123456789012:role/eh-federation",
                "11111111-1111-1111-1111-111111111111",
                "https://login.microsoftonline.com/t/oauth2/v2.0/token",
                "https://ns.servicebus.windows.net/.default", stsHeaderOverrides);
        final Properties properties = new Properties();

        KafkaSecurityConfigurer.setAuthProperties(properties, config, mock(AwsCredentialsSupplier.class), LOG);

        final String jaas = properties.getProperty("sasl.jaas.config");
        assertThat(jaas, containsString("azureFederatedStsHeaderOverrides=\""));
        final String encoded = jaas.replaceAll(".*azureFederatedStsHeaderOverrides=\"([^\"]*)\".*", "$1");
        final String decodedJson = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        final Map<String, String> decoded = new ObjectMapper()
                .readValue(decodedJson, new TypeReference<Map<String, String>>() {});
        assertThat(decoded, equalTo(stsHeaderOverrides));
    }

    @Test
    void setAuthProperties_azureFederatedWithoutStsHeaderOverrides_omitsJaasOption() throws Exception {
        final KafkaSourceConfig config = azureFederatedConfig("us-east-1",
                "arn:aws:iam::123456789012:role/eh-federation",
                "11111111-1111-1111-1111-111111111111",
                "https://login.microsoftonline.com/t/oauth2/v2.0/token",
                "https://ns.servicebus.windows.net/.default");
        final Properties properties = new Properties();

        KafkaSecurityConfigurer.setAuthProperties(properties, config, mock(AwsCredentialsSupplier.class), LOG);

        assertThat(properties.getProperty("sasl.jaas.config"),
                not(containsString("azureFederatedStsHeaderOverrides")));
    }

    private KafkaSourceConfig azureFederatedConfig(final String region, final String stsRoleArn,
            final String clientId, final String tokenEndpoint, final String scope)
            throws NoSuchFieldException, IllegalAccessException {
        return azureFederatedConfig(region, stsRoleArn, clientId, tokenEndpoint, scope, null);
    }

    private KafkaSourceConfig azureFederatedConfig(final String region, final String stsRoleArn,
            final String clientId, final String tokenEndpoint, final String scope,
            final Map<String, String> stsHeaderOverrides)
            throws NoSuchFieldException, IllegalAccessException {
        final AzureFederatedAuthConfig azureFederatedAuthConfig = new AzureFederatedAuthConfig();
        setField(AzureFederatedAuthConfig.class, azureFederatedAuthConfig, "clientId", clientId);
        setField(AzureFederatedAuthConfig.class, azureFederatedAuthConfig, "tokenEndpoint", tokenEndpoint);
        setField(AzureFederatedAuthConfig.class, azureFederatedAuthConfig, "scope", scope);
        final AuthConfig.SaslAuthConfig localSaslAuthConfig = new AuthConfig.SaslAuthConfig();
        setField(AuthConfig.SaslAuthConfig.class, localSaslAuthConfig, "azureFederatedAuthConfig", azureFederatedAuthConfig);
        final AuthConfig localAuthConfig = new AuthConfig();
        setField(AuthConfig.class, localAuthConfig, "saslAuthConfig", localSaslAuthConfig);
        final AwsConfig awsConfig = new AwsConfig();
        setField(AwsConfig.class, awsConfig, "region", region);
        if (stsRoleArn != null) {
            setField(AwsConfig.class, awsConfig, "stsRoleArn", stsRoleArn);
        }
        if (stsHeaderOverrides != null) {
            setField(AwsConfig.class, awsConfig, "awsStsHeaderOverrides", stsHeaderOverrides);
        }
        final KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig();
        setField(KafkaSourceConfig.class, kafkaSourceConfig, "authConfig", localAuthConfig);
        setField(KafkaSourceConfig.class, kafkaSourceConfig, "awsConfig", awsConfig);
        kafkaSourceConfig.setBootStrapServers(List.of("broker:9093"));
        return kafkaSourceConfig;
    }

    private KafkaSourceConfig azureFederatedSourceConfig(final AwsConfig awsConfig)
            throws NoSuchFieldException, IllegalAccessException {
        final AuthConfig.SaslAuthConfig localSaslAuthConfig = new AuthConfig.SaslAuthConfig();
        setField(AuthConfig.SaslAuthConfig.class, localSaslAuthConfig, "azureFederatedAuthConfig",
                new AzureFederatedAuthConfig());
        final AuthConfig localAuthConfig = new AuthConfig();
        setField(AuthConfig.class, localAuthConfig, "saslAuthConfig", localSaslAuthConfig);
        final KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig();
        setField(KafkaSourceConfig.class, kafkaSourceConfig, "authConfig", localAuthConfig);
        if (awsConfig != null) {
            setField(KafkaSourceConfig.class, kafkaSourceConfig, "awsConfig", awsConfig);
        }
        return kafkaSourceConfig;
    }

    private KafkaSourceConfig createKafkaSinkConfig(final String fileName) throws IOException {
        final Yaml yaml = new Yaml();
        final FileReader fileReader = new FileReader(Objects.requireNonNull(getClass().getClassLoader()
                .getResource(fileName)).getFile());
        final Map<String, Map<String, Map<String, Map<String, Object>>>> data = yaml.load(fileReader);
        final Map<String, Map<String, Map<String, Object>>> logPipelineMap = data.get("log-pipeline");
        final Map<String, Map<String, Object>> sourceMap = logPipelineMap.get("source");
        final Map<String, Object> kafkaConfigMap = sourceMap.get("kafka");
        final ObjectMapper mapper = new ObjectMapper();
        final String json = mapper.writeValueAsString(kafkaConfigMap);
        final Reader reader = new StringReader(json);
        return mapper.readValue(reader, KafkaSourceConfig.class);
    }
}
