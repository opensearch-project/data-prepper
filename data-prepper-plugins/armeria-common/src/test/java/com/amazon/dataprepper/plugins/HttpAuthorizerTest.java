/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class HttpAuthorizerTest {
    @Test
    public void basicAuthValidUsernamePassword() {
        final HttpBasicAuthenticationConfig config = new HttpBasicAuthenticationConfig("username", "password");
        final HttpAuthorizer authorizer = new HttpAuthorizer(config);
        final String encodedAuth = "Basic dXNlcm5hbWU6cGFzc3dvcmQ=";

        assertThat(authorizer.authenticate(encodedAuth), equalTo(true));
    }

    @Test
    public void basicAuthInvalidUsernamePassword() {
        final HttpBasicAuthenticationConfig config = new HttpBasicAuthenticationConfig("username", "password");
        final HttpAuthorizer authorizer = new HttpAuthorizer(config);
        final String encodedAuth = "Basic dXNlcm5hbWU6cGFzc3dvcmQy";

        assertThat(authorizer.authenticate(encodedAuth), equalTo(false));
    }
}
