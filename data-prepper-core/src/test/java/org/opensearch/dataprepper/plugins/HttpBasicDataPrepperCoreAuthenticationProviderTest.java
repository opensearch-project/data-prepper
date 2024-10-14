/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.BasicAuthenticator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.pipeline.server.HttpBasicAuthenticationConfig;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

class HttpBasicDataPrepperCoreAuthenticationProviderTest {

    private HttpBasicAuthenticationConfig authenticationConfig;
    private String username;
    private String password;

    @BeforeEach
    void setUp() {
        authenticationConfig = mock(HttpBasicAuthenticationConfig.class);

        username = UUID.randomUUID().toString();
        password = UUID.randomUUID().toString();
        when(authenticationConfig.getUsername()).thenReturn(username);
        when(authenticationConfig.getPassword()).thenReturn(password);
    }

    private HttpBasicDataPrepperCoreAuthenticationProvider createObjectUnderTest() {
        return new HttpBasicDataPrepperCoreAuthenticationProvider(authenticationConfig);
    }

    @Test
    void constructor_with_null_Config_throws() {
        authenticationConfig = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_with_null_username_throws() {
        reset(authenticationConfig);
        when(authenticationConfig.getPassword()).thenReturn(password);
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_with_null_password_throws() {
        reset(authenticationConfig);
        when(authenticationConfig.getUsername()).thenReturn(username);
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void getAuthenticator_should_return_Authenticator_with_core_Realm() {
        final Authenticator authenticator = createObjectUnderTest().getAuthenticator();

        assertThat(authenticator, notNullValue());

        assertThat(authenticator, instanceOf(BasicAuthenticator.class));

        final BasicAuthenticator basicAuthenticator = (BasicAuthenticator) authenticator;

        assertThat(basicAuthenticator.getRealm(), equalTo("core"));
    }

    @Test
    void getAuthenticator_should_return_Authenticator_which_returns_true_for_matching_username_and_password() {
        final Authenticator authenticator = createObjectUnderTest().getAuthenticator();

        assertThat(authenticator, notNullValue());

        assertThat(authenticator, instanceOf(BasicAuthenticator.class));

        final BasicAuthenticator basicAuthenticator = (BasicAuthenticator) authenticator;

        assertThat(basicAuthenticator.checkCredentials(username, password), equalTo(true));
    }

    @Test
    void getAuthenticator_should_return_Authenticator_which_returns_false_for_incorrect_username() {
        final Authenticator authenticator = createObjectUnderTest().getAuthenticator();

        assertThat(authenticator, notNullValue());

        assertThat(authenticator, instanceOf(BasicAuthenticator.class));

        final BasicAuthenticator basicAuthenticator = (BasicAuthenticator) authenticator;

        assertThat(basicAuthenticator.checkCredentials(UUID.randomUUID().toString(), password), equalTo(false));
    }

    @Test
    void getAuthenticator_should_return_Authenticator_which_returns_false_for_incorrect_password() {
        final Authenticator authenticator = createObjectUnderTest().getAuthenticator();

        assertThat(authenticator, notNullValue());

        assertThat(authenticator, instanceOf(BasicAuthenticator.class));

        final BasicAuthenticator basicAuthenticator = (BasicAuthenticator) authenticator;

        assertThat(basicAuthenticator.checkCredentials(username, UUID.randomUUID().toString()), equalTo(false));
    }

    @Test
    void getAuthenticator_should_return_Authenticator_which_returns_false_for_null_username() {
        final Authenticator authenticator = createObjectUnderTest().getAuthenticator();

        assertThat(authenticator, notNullValue());

        assertThat(authenticator, instanceOf(BasicAuthenticator.class));

        final BasicAuthenticator basicAuthenticator = (BasicAuthenticator) authenticator;

        assertThat(basicAuthenticator.checkCredentials(null, password), equalTo(false));
    }

    @Test
    void getAuthenticator_should_return_Authenticator_which_returns_false_for_null_password() {
        final Authenticator authenticator = createObjectUnderTest().getAuthenticator();

        assertThat(authenticator, notNullValue());

        assertThat(authenticator, instanceOf(BasicAuthenticator.class));

        final BasicAuthenticator basicAuthenticator = (BasicAuthenticator) authenticator;

        assertThat(basicAuthenticator.checkCredentials(username, null), equalTo(false));
    }
}