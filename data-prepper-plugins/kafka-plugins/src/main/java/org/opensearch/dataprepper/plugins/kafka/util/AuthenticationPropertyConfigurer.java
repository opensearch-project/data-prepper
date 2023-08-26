/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.util;

import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;

import java.util.Base64;
import java.util.Properties;

/**
 * * This is static property configurer dedicated to authencation related information given in pipeline.yml
 */

public class AuthenticationPropertyConfigurer {

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

    private static final String SASL_SSL_PROTOCOL = "SASL_SSL";

    public static void setSaslPlainTextProperties(final PlainTextAuthConfig plainTextAuthConfig,
                                                  final Properties properties) {

        String username = plainTextAuthConfig.getUsername();
        String password = plainTextAuthConfig.getPassword();
        properties.put(SASL_MECHANISM, PLAIN_MECHANISM);
        properties.put(SASL_JAS_CONFIG, String.format(PLAINTEXT_JAASCONFIG, username, password));
        properties.put(SASL_SECURITY_PROTOCOL, SASL_PLAINTEXT_PROTOCOL);
    }

    public static void setOauthProperties(final OAuthConfig oAuthConfig,
                                          final Properties properties) {
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
        final String extensionLogicalCluster = oAuthConfig.getExtensionLogicalCluster();
        final String extensionIdentityPoolId = oAuthConfig.getExtensionIdentityPoolId();

        properties.put(SASL_MECHANISM, saslMechanism);
        properties.put(SASL_SECURITY_PROTOCOL, securityProtocol);
        properties.put(SASL_TOKEN_ENDPOINT_URL, tokenEndPointURL);
        properties.put(SASL_CALLBACK_HANDLER_CLASS, loginCallBackHandler);

        populateJwksEndpoint(properties, oauthJwksEndpointURL);
        String instrospect_properties = getInstrospectProperties(oauthAuthorizationToken, oauthIntrospectEndpoint, oauthJwksEndpointURL, introspectServer);
        String jass_config = createJassConfig(oauthClientId, oauthClientSecret, oauthLoginServer,
                oauthLoginEndpoint, oauthLoginGrantType, oauthLoginScope, oauthAuthorizationToken,
                extensionLogicalCluster, extensionIdentityPoolId, instrospect_properties);

        properties.put(SASL_JAS_CONFIG, jass_config);
    }

    private static void populateJwksEndpoint(final Properties properties, final String oauthJwksEndpointURL) {
        if (oauthJwksEndpointURL != null && !oauthJwksEndpointURL.isEmpty() && !oauthJwksEndpointURL.isBlank()) {
            properties.put(SASL_JWKS_ENDPOINT_URL, oauthJwksEndpointURL);
        }
    }

    private static String getInstrospectProperties(final String oauthAuthorizationToken, final String oauthIntrospectEndpoint,
                                                   final String oauthJwksEndpointURL, final String introspectServer) {
        String instrospect_properties = "";
        if (oauthJwksEndpointURL != null && !oauthIntrospectEndpoint.isBlank() && !oauthIntrospectEndpoint.isEmpty()) {
            instrospect_properties = String.format(INSTROSPECT_SERVER_PROPERTIES, introspectServer, oauthIntrospectEndpoint, oauthAuthorizationToken);
        }
        return instrospect_properties;
    }

    private static String createJassConfig(final String oauthClientId, final String oauthClientSecret, final String oauthLoginServer,
                                           final String oauthLoginEndpoint, final String oauthLoginGrantType, final String oauthLoginScope,
                                           final String oauthAuthorizationToken, final String extensionLogicalCluster,
                                           final String extensionIdentityPoolId, final String instrospect_properties) {
        String jass_config = String.format(OAUTH_JAASCONFIG, oauthClientId, oauthClientSecret, oauthLoginScope, oauthLoginServer,
                oauthLoginEndpoint, oauthLoginGrantType, oauthLoginScope, oauthAuthorizationToken, instrospect_properties);

        jass_config = getJassConfigWithClusterInforation(extensionLogicalCluster, extensionIdentityPoolId, jass_config);
        return jass_config;
    }

    private static String getJassConfigWithClusterInforation(final String extensionLogicalCluster, final String extensionIdentityPoolId,
                                                             String jass_config) {
        if (extensionLogicalCluster != null && extensionIdentityPoolId != null) {
            String extensionValue = "extension_logicalCluster= \"%s\" extension_identityPoolId=" + " \"%s\";";
            jass_config = jass_config.replace(";", " ");
            jass_config = jass_config + String.format(extensionValue, extensionLogicalCluster, extensionIdentityPoolId);
        }
        return jass_config;
    }

    public static void setSaslPlainProperties(final PlainTextAuthConfig plainTextAuthConfig,
                                              final Properties properties) {

        String username = plainTextAuthConfig.getUsername();
        String password = plainTextAuthConfig.getPassword();
        properties.put(SASL_MECHANISM, PLAIN_MECHANISM);
        properties.put(SASL_JAS_CONFIG, String.format(PLAINTEXT_JAASCONFIG, username, password));
        properties.put(SASL_SECURITY_PROTOCOL, SASL_SSL_PROTOCOL);

    }

}

