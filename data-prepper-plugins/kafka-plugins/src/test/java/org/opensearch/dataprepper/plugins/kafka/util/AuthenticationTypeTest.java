package org.opensearch.dataprepper.plugins.kafka.util;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class AuthenticationTypeTest {

    @ParameterizedTest
    @EnumSource(AuthenticationType.class)
    void getAuthTypeByName(final AuthenticationType name) {
        assertThat(AuthenticationType.getAuthTypeByName(name.name()), is(name));
    }
}