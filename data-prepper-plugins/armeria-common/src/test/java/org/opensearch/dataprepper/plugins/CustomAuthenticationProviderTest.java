package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.armeria.authentication.CustomAuthenticationProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CustomAuthenticationProviderTest {

    @Mock
    private CustomAuthenticationProvider customAuthenticationProvider;

    @Test
    public void testCustomAuthenticationProvider() {
        when(customAuthenticationProvider.getHttpAuthenticationService()).thenCallRealMethod();

        var result = customAuthenticationProvider.getHttpAuthenticationService();
        Assertions.assertNotNull(result);
        Assertions.assertFalse(result.isPresent());
    }
}
