/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GrpcAuthenticationProviderTest {

    @Mock
    private GrpcAuthenticationProvider provider;

    @Test
    public void confirmDefaultBehavior() {
        when(provider.getHttpAuthenticationService()).thenCallRealMethod();

        var result = provider.getHttpAuthenticationService();
        Assertions.assertNotNull(result);
        Assertions.assertFalse(result.isPresent());
    }
}
