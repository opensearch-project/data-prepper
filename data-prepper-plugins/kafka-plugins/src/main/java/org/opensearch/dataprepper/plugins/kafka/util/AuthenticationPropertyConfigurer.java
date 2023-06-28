/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.util;

import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;

import java.util.Base64;
import java.util.Properties;

/**
 * * This is static property configurer dedicated to authencation related information given in pipeline.yml
 */

public class AuthenticationPropertyConfigurer {

    private static final String SESSION_TIMEOUT_MS_CONFIG = "30000";

    private static final String SASL_MECHANISM = "sasl.mechanism";

    private static final String SASL_SECURITY_PROTOCOL = "security.protocol";

    private static final String SASL_JAS_CONFIG = "sasl.jaas.config";

    private static final String SASL_CALLBACK_HANDLER_CLASS = "sasl.login.callback.handler.class";

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

    private static final String PLAIN_MECHANISM = "PLAIN";

    private static final String SASL_PLAINTEXT_PROTOCOL = "SASL_PLAINTEXT";


    public static void setSaslPlainTextProperties(final KafkaSinkConfig kafkaSinkConfig,
                                                  final Properties properties) {

        String username = kafkaSinkConfig.getAuthConfig().getPlainTextAuthConfig().getUsername();
        String password = kafkaSinkConfig.getAuthConfig().getPlainTextAuthConfig().getPassword();
        properties.put(SASL_MECHANISM, PLAIN_MECHANISM);
        properties.put(SASL_JAS_CONFIG, String.format(PLAINTEXT_JAASCONFIG, username, password));
        properties.put(SASL_SECURITY_PROTOCOL, SASL_PLAINTEXT_PROTOCOL);
    }

    public static void setOauthProperties(final KafkaSinkConfig kafkaSinkConfig,
                                          final Properties properties) {
        String oauthClientId = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthClientId();
        String oauthClientSecret = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthClientSecret();
        String oauthLoginServer = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthLoginServer();
        String oauthLoginEndpoint = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthLoginEndpoint();
        String oauthLoginGrantType = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthLoginGrantType();
        String oauthLoginScope = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthLoginScope();
        String oauthAuthorizationToken = Base64.getEncoder().encodeToString((oauthClientId + ":" + oauthClientSecret).getBytes());
        String oauthIntrospectEndpoint = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthIntrospectEndpoint();
        String tokenEndPointURL = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthTokenEndpointURL();
        String saslMechanism = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthSaslMechanism();
        String securityProtocol = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthSecurityProtocol();
        String loginCallBackHandler = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthSaslLoginCallbackHandlerClass();
        String oauthJwksEndpointURL = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthJwksEndpointURL();
        String introspectServer = kafkaSinkConfig.getAuthConfig().getoAuthConfig().getOauthIntrospectServer();


        properties.put(SASL_MECHANISM, saslMechanism);
        properties.put(SASL_SECURITY_PROTOCOL, securityProtocol);
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

        properties.put(SASL_JAS_CONFIG, jass_config);
    }
}

