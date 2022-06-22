/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
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

        Assertions.assertNotNull(provider.getHttpAuthenticationService());
        Assertions.assertFalse(provider.getHttpAuthenticationService().isPresent());
    }
}
