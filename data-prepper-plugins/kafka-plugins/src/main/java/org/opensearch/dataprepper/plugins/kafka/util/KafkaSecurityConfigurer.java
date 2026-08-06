/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.util;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.plugins.kafka.authenticator.AwsCredentialsSupplierProvider;
import org.opensearch.dataprepper.plugins.kafka.authenticator.AzureFederatedCallbackHandler;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicSaslClientCallbackHandler;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicBasicCredentialsProvider;
import org.opensearch.dataprepper.plugins.kafka.common.aws.AwsContext;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsIamAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AzureFederatedAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaRegistryType;
import org.opensearch.dataprepper.plugins.kafka.configuration.ScramAuthConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.utils.ExponentialBackoff;
import java.time.Duration;

import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersRequest;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersResponse;
import software.amazon.awssdk.services.kafka.model.KafkaException;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.model.StsException;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.regions.Region;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.glue.model.Compatibility;

import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/**
 * * This is static property configure dedicated to authentication related information given in pipeline.yml
 */

public class KafkaSecurityConfigurer {

    private static final String SASL_MECHANISM = "sasl.mechanism";

    private static final String SECURITY_PROTOCOL = "security.protocol";

    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

    private static final String SASL_CALLBACK_HANDLER_CLASS = "sasl.login.callback.handler.class";
    private static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS = "sasl.client.callback.handler.class";

    private static final String SASL_JWKS_ENDPOINT_URL = "sasl.oauthbearer.jwks.endpoint.url";

    private static final String SASL_TOKEN_ENDPOINT_URL = "sasl.oauthbearer.token.endpoint.url";

    private static final String PLAINTEXT_JAASCONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username= \"%s\" password=  " +
            " \"%s\";";
    private static final String OAUTH_JAASCONFIG = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId='"
            + "%s" + "' clientSecret='" + "%s" + "' scope='" + "%s" + "' OAUTH_LOGIN_SERVER='" + "%s" +
            "' OAUTH_LOGIN_ENDPOINT='" + "%s" + "' OAUT_LOGIN_GRANT_TYPE=" + "%s" +
            " OAUTH_LOGIN_SCOPE=%s OAUTH_AUTHORIZATION='Basic " + "%s" + "';";

    private static final String INSTROSPECT_SERVER_PROPERTIES = " OAUTH_INTROSPECT_SERVER='"
            + "%s" + "' OAUTH_INTROSPECT_ENDPOINT='" + "%s" + "' " +
            "OAUTH_INTROSPECT_AUTHORIZATION='Basic " + "%s";

    private static final String AZURE_FEDERATED_HANDLER_CLASS =
            "org.opensearch.dataprepper.plugins.kafka.authenticator.AzureFederatedCallbackHandler";

    private static final String PLAIN_MECHANISM = "PLAIN";
    private static final String OAUTHBEARER_MECHANISM = "OAUTHBEARER";

    private static final String SASL_SSL_PROTOCOL = "SASL_SSL";

    private static final String SASL_PLAINTEXT_PROTOCOL = "SASL_PLAINTEXT";

    private static final String PLAINTEXT_PROTOCOL = "PLAINTEXT";

    private static final String REGISTRY_BASIC_AUTH_USER_INFO = "schema.registry.basic.auth.user.info";

    private static final int MAX_KAFKA_CLIENT_RETRIES = 360; // for one hour every 10 seconds
    private static final String SSL_ENGINE_FACTORY_CLASS = "ssl.engine.factory.class";
    private static final String CERTIFICATE_CONTENT = "certificateContent";
    private static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
    private static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static AwsCredentialsProvider mskCredentialsProvider;
    private static AwsCredentialsProvider awsGlueCredentialsProvider;
    private static GlueSchemaRegistryKafkaDeserializer glueDeserializer;


    /*public static void setSaslPlainTextProperties(final KafkaSourceConfig kafkaSourConfig,
                                                  final Properties properties) {
        final AuthConfig.SaslAuthConfig saslAuthConfig = kafkaSourConfig.getAuthConfig().getSaslAuthConfig();
        String username = saslAuthConfig.getPlainTextAuthConfig().getUsername();
        String password = saslAuthConfig.getPlainTextAuthConfig().getPassword();
        if (saslAuthConfig.getPlainTextAuthConfig() != null) {
            properties.put(SASL_MECHANISM, PLAIN_MECHANISM);
        }
        if (saslAuthConfig!= null) {
            if (StringUtils.isNotEmpty(saslAuthConfig.getPlainTextAuthConfig().getPlaintext()) &&
                    PLAINTEXT_PROTOCOL.equalsIgnoreCase(saslAuthConfig.getAuthProtocolConfig().getPlaintext())) {
                properties.put(SECURITY_PROTOCOL, PLAINTEXT_PROTOCOL);
            } else if (StringUtils.isNotEmpty(saslAuthConfig.getAuthProtocolConfig().getPlaintext()) &&
                    SASL_PLAINTEXT_PROTOCOL.equalsIgnoreCase(saslAuthConfig.getAuthProtocolConfig().getPlaintext())) {
                properties.put(SECURITY_PROTOCOL, SASL_PLAINTEXT_PROTOCOL);
            }
        }
        properties.put(SASL_JAAS_CONFIG, String.format(PLAINTEXT_JAASCONFIG, username, password));
    }*/

    private static void setPlainTextAuthProperties(final Properties properties, final PlainTextAuthConfig plainTextAuthConfig,
                                                   final EncryptionConfig encryptionConfig) {
        final String username = plainTextAuthConfig.getUsername();
        final String password = plainTextAuthConfig.getPassword();
        properties.put(SASL_MECHANISM, "PLAIN");
        properties.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        if (checkEncryptionType(encryptionConfig, EncryptionType.SSL)) {
            properties.put(SECURITY_PROTOCOL, SASL_SSL_PROTOCOL);
            setSecurityProtocolSSLProperties(properties, encryptionConfig);
        } else { // EncryptionType.NONE
            properties.put(SECURITY_PROTOCOL, "SASL_PLAINTEXT");
        }
    }

    private static void setScramAuthProperties(final Properties properties, final ScramAuthConfig scramAuthConfig,
                                                   final EncryptionConfig encryptionConfig) {
        final String username = scramAuthConfig.getUsername();
        final String password = scramAuthConfig.getPassword();
        final String mechanism = scramAuthConfig.getMechanism();
        properties.put(SASL_MECHANISM, mechanism);
        properties.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        if (checkEncryptionType(encryptionConfig, EncryptionType.SSL)) {
            properties.put(SECURITY_PROTOCOL, SASL_SSL_PROTOCOL);
            setSecurityProtocolSSLProperties(properties, encryptionConfig);
        } else { // EncryptionType.NONE
            properties.put(SECURITY_PROTOCOL, "SASL_PLAINTEXT");
        }
    }

    private static void setSecurityProtocolSSLProperties(final Properties properties, final EncryptionConfig encryptionConfig) {
        if (Objects.nonNull(encryptionConfig.getCertificate())) {
            setCustomSslProperties(properties, encryptionConfig.getCertificate());
        } else if (Objects.nonNull(encryptionConfig.getTrustStoreFilePath()) &&
                Objects.nonNull(encryptionConfig.getTrustStorePassword())) {
            setTruststoreProperties(properties, encryptionConfig);
        }
    }

    private static void setCustomSslProperties(final Properties properties, final String certificateContent) {
        properties.put(CERTIFICATE_CONTENT, certificateContent);
        properties.put(SSL_ENGINE_FACTORY_CLASS, CustomClientSslEngineFactory.class);
    }

    private static void setTruststoreProperties(final Properties properties, final EncryptionConfig encryptionConfig) {
        properties.put(SSL_TRUSTSTORE_LOCATION, encryptionConfig.getTrustStoreFilePath());
        properties.put(SSL_TRUSTSTORE_PASSWORD, encryptionConfig.getTrustStorePassword());
    }

    public static void setOauthProperties(final KafkaClusterAuthConfig kafkaClusterAuthConfig,
                                          final Properties properties) {
        final OAuthConfig oAuthConfig = kafkaClusterAuthConfig.getAuthConfig().getSaslAuthConfig().getOAuthConfig();
        final String oauthClientId = oAuthConfig.getOauthClientId();
        final String oauthClientSecret = oAuthConfig.getOauthClientSecret();
        final String oauthLoginServer = oAuthConfig.getOauthLoginServer();
        final String oauthLoginEndpoint = oAuthConfig.getOauthLoginEndpoint();
        final String oauthLoginGrantType = oAuthConfig.getOauthLoginGrantType();
        final String oauthLoginScope = oAuthConfig.getOauthLoginScope();
        final String oauthAuthorizationToken = Base64.getEncoder().encodeToString((oauthClientId + ":" + oauthClientSecret).getBytes());
        final String oauthIntrospectEndpoint = oAuthConfig.getOauthIntrospectEndpoint();
        final String tokenEndPointURL = oAuthConfig.getOauthTokenEndpointURL();
        final String saslMechanism = oAuthConfig.getOauthSaslMechanism();
        final String securityProtocol = oAuthConfig.getOauthSecurityProtocol();
        final String loginCallBackHandler = oAuthConfig.getOauthSaslLoginCallbackHandlerClass();
        final String oauthJwksEndpointURL = oAuthConfig.getOauthJwksEndpointURL();
        final String introspectServer = oAuthConfig.getOauthIntrospectServer();


        properties.put(SASL_MECHANISM, saslMechanism);
        properties.put(SECURITY_PROTOCOL, securityProtocol);
        properties.put(SASL_TOKEN_ENDPOINT_URL, tokenEndPointURL);
        properties.put(SASL_CALLBACK_HANDLER_CLASS, loginCallBackHandler);


        if (oauthJwksEndpointURL != null && !oauthJwksEndpointURL.isEmpty() && !oauthJwksEndpointURL.isBlank()) {
            properties.put(SASL_JWKS_ENDPOINT_URL, oauthJwksEndpointURL);
        }

        String instrospect_properties = "";
        if (oauthJwksEndpointURL != null && !oauthIntrospectEndpoint.isBlank() && !oauthIntrospectEndpoint.isEmpty()) {
            instrospect_properties = String.format(INSTROSPECT_SERVER_PROPERTIES, introspectServer, oauthIntrospectEndpoint, oauthAuthorizationToken);
        }

        String jass_config = String.format(OAUTH_JAASCONFIG, oauthClientId, oauthClientSecret, oauthLoginScope, oauthLoginServer,
                oauthLoginEndpoint, oauthLoginGrantType, oauthLoginScope, oauthAuthorizationToken, instrospect_properties);

        if (kafkaClusterAuthConfig instanceof KafkaSourceConfig &&
                "USER_INFO".equalsIgnoreCase(((KafkaSourceConfig) kafkaClusterAuthConfig).getSchemaConfig().getBasicAuthCredentialsSource())) {
            final SchemaConfig schemaConfig = ((KafkaSourceConfig) kafkaClusterAuthConfig).getSchemaConfig();
            final String apiKey = schemaConfig.getSchemaRegistryApiKey();
            final String apiSecret = schemaConfig.getSchemaRegistryApiSecret();
            final String extensionLogicalCluster = oAuthConfig.getExtensionLogicalCluster();
            final String extensionIdentityPoolId = oAuthConfig.getExtensionIdentityPoolId();
            properties.put(REGISTRY_BASIC_AUTH_USER_INFO, apiKey + ":" + apiSecret);
            properties.put("basic.auth.credentials.source", "USER_INFO");
            String extensionValue = "extension_logicalCluster= \"%s\" extension_identityPoolId=  " + " \"%s\";";
            jass_config = jass_config.replace(";", " ");
            jass_config += String.format(extensionValue, extensionLogicalCluster, extensionIdentityPoolId);
        }
        properties.put(SASL_JAAS_CONFIG, jass_config);
    }

    public static void setAwsIamAuthProperties(Properties properties, final AwsIamAuthConfig awsIamAuthConfig, final AwsConfig awsConfig) {
        properties.put(SECURITY_PROTOCOL, SASL_SSL_PROTOCOL);
        properties.put(SASL_MECHANISM, "AWS_MSK_IAM");
        properties.put(SASL_CLIENT_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        if (awsIamAuthConfig == AwsIamAuthConfig.ROLE) {
            if (Objects.isNull(awsConfig)) {
                throw new RuntimeException("AWS Config needs to be specified when sasl/aws_msk_iam is set to \"role\"");
            }
            String baseIamAuthConfig = "software.amazon.msk.auth.iam.IAMLoginModule required " +
                "awsRoleArn=\"%s\" " +
                "awsStsRegion=\"%s\"";

            baseIamAuthConfig = String.format(baseIamAuthConfig, awsConfig.getStsRoleArn(), awsConfig.getRegion());

            if (Objects.nonNull(awsConfig.getStsRoleSessionName())) {
                baseIamAuthConfig += String.format(" awsRoleSessionName=\"%s\"", awsConfig.getStsRoleSessionName());
            }

            baseIamAuthConfig += ";";
            properties.put(SASL_JAAS_CONFIG, baseIamAuthConfig);
        } else if (awsIamAuthConfig == AwsIamAuthConfig.DEFAULT) {
            properties.put(SASL_JAAS_CONFIG,
                    "software.amazon.msk.auth.iam.IAMLoginModule required;");
        }
    }

    private static void setAzureFederatedAuthProperties(final Properties properties,
            final AzureFederatedAuthConfig azureFederatedAuthConfig, final AwsConfig awsConfig,
            final AwsCredentialsSupplier awsCredentialsSupplier) {
        final boolean hasAwsConfig = Objects.nonNull(awsConfig);
        // Region may come from the pipeline aws config or, if absent there, the data-prepper-config.yaml default.
        final String region = hasAwsConfig && Objects.nonNull(awsConfig.getRegion())
                ? awsConfig.getRegion()
                : awsCredentialsSupplier.getDefaultRegion().map(Region::id).orElse(null);
        if (Objects.isNull(region)) {
            throw new RuntimeException("azure_federated requires a region in the pipeline aws config or data-prepper-config.yaml");
        }
        properties.put(SASL_MECHANISM, OAUTHBEARER_MECHANISM);
        properties.put(SECURITY_PROTOCOL, SASL_SSL_PROTOCOL);
        properties.put(SASL_CALLBACK_HANDLER_CLASS, AZURE_FEDERATED_HANDLER_CLASS);
        final StringBuilder jaas = new StringBuilder(
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ");
        appendJaasOption(jaas, AzureFederatedCallbackHandler.OPT_REGION, region);
        if (hasAwsConfig && Objects.nonNull(awsConfig.getStsRoleArn())) {
            appendJaasOption(jaas, AzureFederatedCallbackHandler.OPT_STS_ROLE_ARN, awsConfig.getStsRoleArn());
        }
        if (hasAwsConfig && Objects.nonNull(awsConfig.getAwsStsHeaderOverrides())
                && !awsConfig.getAwsStsHeaderOverrides().isEmpty()) {
            appendJaasOption(jaas, AzureFederatedCallbackHandler.OPT_STS_HEADER_OVERRIDES,
                    encodeStsHeaderOverrides(awsConfig.getAwsStsHeaderOverrides()));
        }
        appendJaasOption(jaas, AzureFederatedCallbackHandler.OPT_TOKEN_ENDPOINT, azureFederatedAuthConfig.getTokenEndpoint());
        appendJaasOption(jaas, AzureFederatedCallbackHandler.OPT_CLIENT_ID, azureFederatedAuthConfig.getClientId());
        jaas.append(AzureFederatedCallbackHandler.OPT_SCOPE).append("=\"")
                .append(azureFederatedAuthConfig.getScope()).append("\";");
        properties.put(SASL_JAAS_CONFIG, jaas.toString());
    }

    private static void appendJaasOption(final StringBuilder jaas, final String key, final String value) {
        jaas.append(key).append("=\"").append(value).append("\" ");
    }

    private static String encodeStsHeaderOverrides(final Map<String, String> stsHeaderOverrides) {
        try {
            final String json = OBJECT_MAPPER.writeValueAsString(stsHeaderOverrides);
            return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        } catch (final JsonProcessingException e) {
            throw new RuntimeException("Failed to encode aws.sts_header_overrides for azure_federated", e);
        }
    }

    private static void configureMSKCredentialsProvider(final AuthConfig authConfig, final AwsConfig awsConfig) {
        mskCredentialsProvider = DefaultCredentialsProvider.create();
        if (Objects.nonNull(authConfig) && Objects.nonNull(authConfig.getSaslAuthConfig()) &&
                authConfig.getSaslAuthConfig().getAwsIamAuthConfig() == AwsIamAuthConfig.ROLE) {
            String sessionName = "data-prepper-kafka-session" + UUID.randomUUID();
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(awsConfig.getRegion()))
                    .credentialsProvider(mskCredentialsProvider)
                    .build();
            AssumeRoleRequest.Builder assumeRequestBuilder = AssumeRoleRequest
                    .builder()
                    .roleArn(awsConfig.getStsRoleArn())
                    .roleSessionName(sessionName);
            Map<String, String> headers = awsConfig.getAwsStsHeaderOverrides();
            if (Objects.nonNull(headers)) {
                assumeRequestBuilder.overrideConfiguration(configuration -> {
                    headers.forEach(configuration::putHeader);
                });
            }
            mskCredentialsProvider = StsAssumeRoleCredentialsProvider
                    .builder()
                    .stsClient(stsClient)
                    .refreshRequest(assumeRequestBuilder.build())
                    .build();
        }
    }

    public static String getBootStrapServersForMsk(final AwsConfig awsConfig,
                                                   final AwsCredentialsProvider mskCredentialsProvider,
                                                   final Logger log) {
        final AwsConfig.AwsMskConfig awsMskConfig = awsConfig.getAwsMskConfig();
        KafkaClient kafkaClient = KafkaClient.builder()
                .credentialsProvider(mskCredentialsProvider)
                .region(Region.of(awsConfig.getRegion()))
                .build();
        final GetBootstrapBrokersRequest request =
                GetBootstrapBrokersRequest
                        .builder()
                        .clusterArn(awsMskConfig.getArn())
                        .build();

        final ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofSeconds(10).toMillis(), 2, Duration.ofMinutes(10).toMillis(), 0);
        int numRetries = 0;
        boolean retryable;
        GetBootstrapBrokersResponse result = null;
        do {
            retryable = false;
            try {
                result = kafkaClient.getBootstrapBrokers(request);
            } catch (StsException e) {
                if (e.statusCode() == 403) {
                    throw new RuntimeException("Access denied when calling STS to get bootstrap server information from MSK. " +
                            "Verify that the role exists and the trust policy is correctly configured.", e);
                }
                long backoffMs = backoff.backoff(numRetries);
                log.info("Failed to get bootstrap server information from MSK due to STS error. Retrying after {} ms (attempt {}/{})",
                        backoffMs, numRetries + 1, MAX_KAFKA_CLIENT_RETRIES, e);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException exp) {}
                retryable = true;
            } catch (KafkaException e) {
                long backoffMs = backoff.backoff(numRetries);
                log.info("Failed to get bootstrap server information from MSK due to Kafka error. Retrying after {} ms (attempt {}/{})",
                        backoffMs, numRetries + 1, MAX_KAFKA_CLIENT_RETRIES, e);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException exp) {}
                retryable = true;
            } catch (Exception e) {
                throw new RuntimeException("Failed to get bootstrap server information from MSK.", e);
            }
        } while (retryable && numRetries++ < MAX_KAFKA_CLIENT_RETRIES);
        if (Objects.isNull(result)) {
            throw new RuntimeException("Failed to get bootstrap server information from MSK after trying multiple times with retryable exceptions.");
        }
        switch (awsMskConfig.getBrokerConnectionType()) {
            case PUBLIC:
                return result.bootstrapBrokerStringPublicSaslIam();
            case MULTI_VPC:
                return result.bootstrapBrokerStringVpcConnectivitySaslIam();
            default:
            case SINGLE_VPC:
                return result.bootstrapBrokerStringSaslIam();
        }
    }
    public static void setDynamicSaslClientCallbackHandler(final Properties properties,
                                                           final KafkaConnectionConfig kafkaConnectionConfig,
                                                           final PluginConfigObservable pluginConfigObservable) {
        final AuthConfig authConfig = kafkaConnectionConfig.getAuthConfig();
        if (Objects.nonNull(authConfig)) {
            AuthConfig.SaslAuthConfig saslAuthConfig = authConfig.getSaslAuthConfig();
            if (Objects.nonNull(saslAuthConfig) && Objects.nonNull(saslAuthConfig.getPlainTextAuthConfig())) {
                final DynamicBasicCredentialsProvider dynamicBasicCredentialsProvider =
                        DynamicBasicCredentialsProvider.getInstance();
                pluginConfigObservable.addPluginConfigObserver(
                        newConfig -> dynamicBasicCredentialsProvider.refresh((KafkaConnectionConfig) newConfig));
                dynamicBasicCredentialsProvider.refresh(kafkaConnectionConfig);
                properties.put(SASL_CLIENT_CALLBACK_HANDLER_CLASS, DynamicSaslClientCallbackHandler.class);
            }
        }
    }
    public static void setAuthProperties(final Properties properties, final KafkaClusterAuthConfig kafkaClusterAuthConfig,
                                         final AwsCredentialsSupplier awsCredentialsSupplier, final Logger log) {
        if (awsCredentialsSupplier != null) {
            AwsCredentialsSupplierProvider.getInstance().set(awsCredentialsSupplier);
        }
        final AwsConfig awsConfig = kafkaClusterAuthConfig.getAwsConfig();
        final AuthConfig authConfig = kafkaClusterAuthConfig.getAuthConfig();
        final EncryptionConfig encryptionConfig = kafkaClusterAuthConfig.getEncryptionConfig();
        configureMSKCredentialsProvider(authConfig, awsConfig);

        String bootstrapServers = "";
        if (Objects.nonNull(kafkaClusterAuthConfig.getBootstrapServers())) {
            bootstrapServers = String.join(",", kafkaClusterAuthConfig.getBootstrapServers());
        }
        if (Objects.nonNull(awsConfig) && Objects.nonNull(awsConfig.getAwsMskConfig())) {
            bootstrapServers = getBootStrapServersForMsk(awsConfig, mskCredentialsProvider, log);
        }

        if (Objects.nonNull(authConfig)) {
            final AuthConfig.SaslAuthConfig saslAuthConfig = authConfig.getSaslAuthConfig();
            if (Objects.nonNull(saslAuthConfig)) {
                final AwsIamAuthConfig awsIamAuthConfig = saslAuthConfig.getAwsIamAuthConfig();
                final ScramAuthConfig scramAuthConfig = saslAuthConfig.getScramAuthConfig();
                final PlainTextAuthConfig plainTextAuthConfig = saslAuthConfig.getPlainTextAuthConfig();

                if (Objects.nonNull(awsIamAuthConfig)) {
                    if (checkEncryptionType(encryptionConfig, EncryptionType.NONE)) {
                        throw new RuntimeException("Encryption Config must be SSL to use IAM authentication mechanism");
                    }
                    setAwsIamAuthProperties(properties, awsIamAuthConfig, awsConfig);
                } else if (Objects.nonNull(saslAuthConfig.getOAuthConfig())) {
                    setOauthProperties(kafkaClusterAuthConfig, properties);
                } else if (Objects.nonNull(scramAuthConfig) && Objects.nonNull(kafkaClusterAuthConfig.getEncryptionConfig())) {
                    setScramAuthProperties(properties, scramAuthConfig, kafkaClusterAuthConfig.getEncryptionConfig());
                }  else if (Objects.nonNull(plainTextAuthConfig) && Objects.nonNull(kafkaClusterAuthConfig.getEncryptionConfig())) {
                    setPlainTextAuthProperties(properties, plainTextAuthConfig, kafkaClusterAuthConfig.getEncryptionConfig());
                } else if (Objects.nonNull(saslAuthConfig.getAzureFederatedAuthConfig())) {
                    setAzureFederatedAuthProperties(properties, saslAuthConfig.getAzureFederatedAuthConfig(), awsConfig,
                            awsCredentialsSupplier);
                } else {
                    throw new RuntimeException("No SASL auth config specified");
                }
            }
            if (encryptionConfig.getInsecure()) {
                properties.put(SSL_ENGINE_FACTORY_CLASS, InsecureSslEngineFactory.class);
            }
        }
        if (Objects.isNull(authConfig) || Objects.isNull(authConfig.getSaslAuthConfig())) {
            if (checkEncryptionType(encryptionConfig, EncryptionType.SSL)) {
                properties.put(SECURITY_PROTOCOL, "SSL");
                setSecurityProtocolSSLProperties(properties, encryptionConfig);
            }
        }
        if (Objects.isNull(bootstrapServers) || bootstrapServers.isEmpty()) {
            throw new RuntimeException("Bootstrap servers are not specified");
        }

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    private static boolean checkEncryptionType(final EncryptionConfig encryptionConfig, final EncryptionType encryptionType) {
        return Objects.nonNull(encryptionConfig) && encryptionConfig.getType() == encryptionType;
    }

    public static GlueSchemaRegistryKafkaDeserializer getGlueSerializer(
            final KafkaConsumerConfig kafkaConsumerConfig, final AwsContext awsContext) {
        final AwsConfig awsConfig = kafkaConsumerConfig.getAwsConfig();
        awsGlueCredentialsProvider = awsContext.getOrDefault(awsConfig);
        SchemaConfig schemaConfig = kafkaConsumerConfig.getSchemaConfig();
        if (Objects.isNull(schemaConfig) || schemaConfig.getType() != SchemaRegistryType.AWS_GLUE) {
            return null;
        }
        Map<String, Object> configs = new HashMap<>();
        final Region region = awsContext.getRegionOrDefault(awsConfig);
        if (Objects.nonNull(region)) {
            configs.put(AWSSchemaRegistryConstants.AWS_REGION, region.id());
        }
        configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        configs.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, "86400000");
        configs.put(AWSSchemaRegistryConstants.CACHE_SIZE, "10");
        configs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.FULL);
        String registryUrl = kafkaConsumerConfig.getSchemaConfig().getRegistryURL();
        boolean endpointOverride = kafkaConsumerConfig.getSchemaConfig().getOverrideEndpoint();
        if (endpointOverride) {
            configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, registryUrl);
        }
        glueDeserializer = new GlueSchemaRegistryKafkaDeserializer(awsGlueCredentialsProvider, configs);
        return glueDeserializer;
    }
}

