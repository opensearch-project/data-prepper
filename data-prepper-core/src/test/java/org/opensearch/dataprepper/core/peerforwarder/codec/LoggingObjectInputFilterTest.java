/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.codec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ObjectInputFilter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LoggingObjectInputFilterTest {
    @Mock
    private ObjectInputFilter innerFilter;

    @Mock
    private ObjectInputFilter.FilterInfo filterInfo;

    private LoggingObjectInputFilter createObjectUnderTest() {
        return new LoggingObjectInputFilter(innerFilter);
    }

    @Test
    void constructor_throws_if_filter_is_null() {
        innerFilter = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @ParameterizedTest
    @EnumSource(ObjectInputFilter.Status.class)
    void checkInput_returns_innerFilter_status(final ObjectInputFilter.Status innerStatus) {
        when(innerFilter.checkInput(filterInfo)).thenReturn(innerStatus);

        assertThat(createObjectUnderTest().checkInput(filterInfo), equalTo(innerStatus));
    }

    @Test
    void checkInput_gets_the_class_when_rejected() {
        when(innerFilter.checkInput(filterInfo)).thenReturn(ObjectInputFilter.Status.REJECTED);

        createObjectUnderTest().checkInput(filterInfo);

        verify(filterInfo).serialClass();
    }
}