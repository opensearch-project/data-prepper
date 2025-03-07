package org.opensearch.dataprepper.plugins.kafka.util;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginConfigObserver;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicBasicCredentialsProvider;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicSaslClientCallbackHandler;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.KafkaClientBuilder;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersRequest;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersResponse;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.common.config.SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
    @Captor
    private ArgumentCaptor<PluginConfigObserver> pluginConfigObserverArgumentCaptor;

    @Test
    public void testSetAuthPropertiesWithSaslPlainCertificate() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-sasl-ssl-certificate-content.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
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
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is(nullValue()));
        assertThat(props.getProperty("security.protocol"), is("SSL"));
        assertThat(props.getProperty("certificateContent"), is("CERTIFICATE_DATA"));
        assertThat(props.get("ssl.engine.factory.class"), is(CustomClientSslEngineFactory.class));
    }
    @Test
    public void testSetAuthPropertiesWithNoAuthSslNone() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-no-auth-ssl-none.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is(nullValue()));
        assertThat(props.getProperty("security.protocol"), is(nullValue()));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
    }

    @Test
    public void testSetAuthPropertiesWithNoAuthInsecure() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-auth-insecure.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_PLAINTEXT"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(InsecureSslEngineFactory.class));
    }
    @Test
    public void testSetAuthPropertiesAuthSslWithTrustStore() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-sasl-ssl-truststore.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
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
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
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
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
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
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
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
            KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
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
            KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
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
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig(filename);
        final GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = KafkaSecurityConfigurer
                .getGlueSerializer(kafkaSourceConfig);
        assertThat(glueSchemaRegistryKafkaDeserializer, notNullValue());
        assertThat(glueSchemaRegistryKafkaDeserializer.getCredentialProvider(),
                instanceOf(StsAssumeRoleCredentialsProvider.class));
    }

    @Test
    void testGetGlueSerializerWithDefaultCredentialsProvider() throws IOException {
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig(
                "kafka-pipeline-bootstrap-servers-glue-default.yaml");
        final DefaultAwsRegionProviderChain.Builder defaultAwsRegionProviderChainBuilder = mock(
                DefaultAwsRegionProviderChain.Builder.class);
        final DefaultAwsRegionProviderChain defaultAwsRegionProviderChain = mock(DefaultAwsRegionProviderChain.class);
        when(defaultAwsRegionProviderChainBuilder.build()).thenReturn(defaultAwsRegionProviderChain);
        when(defaultAwsRegionProviderChain.getRegion()).thenReturn(Region.US_EAST_1);
        try (MockedStatic<DefaultAwsRegionProviderChain> defaultAwsRegionProviderChainMockedStatic =
                     mockStatic(DefaultAwsRegionProviderChain.class)) {
            defaultAwsRegionProviderChainMockedStatic.when(DefaultAwsRegionProviderChain::builder)
                    .thenReturn(defaultAwsRegionProviderChainBuilder);
            final GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = KafkaSecurityConfigurer
                    .getGlueSerializer(kafkaSourceConfig);
            assertThat(glueSchemaRegistryKafkaDeserializer, notNullValue());
            assertThat(glueSchemaRegistryKafkaDeserializer.getCredentialProvider(),
                    instanceOf(DefaultCredentialsProvider.class));
            assertThat(glueSchemaRegistryKafkaDeserializer
                    .getGlueSchemaRegistryDeserializationFacade()
                    .getGlueSchemaRegistryConfiguration()
                    .getEndPoint(), is(nullValue()));
        }
    }

    @Test
    void testGetGlueSerializerWithDefaultCredentialsProviderAndOverrridenRegistryEndpoint() throws IOException {
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig(
                "kafka-pipeline-bootstrap-servers-glue-override-endpoint.yaml");
        final DefaultAwsRegionProviderChain.Builder defaultAwsRegionProviderChainBuilder = mock(
                DefaultAwsRegionProviderChain.Builder.class);
        final DefaultAwsRegionProviderChain defaultAwsRegionProviderChain = mock(DefaultAwsRegionProviderChain.class);
        when(defaultAwsRegionProviderChainBuilder.build()).thenReturn(defaultAwsRegionProviderChain);
        when(defaultAwsRegionProviderChain.getRegion()).thenReturn(Region.US_EAST_1);
        try (MockedStatic<DefaultAwsRegionProviderChain> defaultAwsRegionProviderChainMockedStatic =
                     mockStatic(DefaultAwsRegionProviderChain.class)) {
            defaultAwsRegionProviderChainMockedStatic.when(DefaultAwsRegionProviderChain::builder)
                    .thenReturn(defaultAwsRegionProviderChainBuilder);
            final GlueSchemaRegistryKafkaDeserializer glueSchemaRegistryKafkaDeserializer = KafkaSecurityConfigurer
                    .getGlueSerializer(kafkaSourceConfig);
            assertThat(glueSchemaRegistryKafkaDeserializer, notNullValue());
            assertThat(glueSchemaRegistryKafkaDeserializer.getCredentialProvider(),
                    instanceOf(DefaultCredentialsProvider.class));
            assertThat(glueSchemaRegistryKafkaDeserializer
                    .getGlueSchemaRegistryDeserializationFacade()
                    .getGlueSchemaRegistryConfiguration()
                    .getEndPoint(), is("http://fake-glue-registry"));
        }
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
