/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ThreadFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CallbackTheadFactoryTest {
    @Mock
    private ThreadFactory delegateThreadFactory;

    @Mock
    private Runnable runnable;

    private CallbackTheadFactory createObjectUnderTest() {
        return new CallbackTheadFactory(delegateThreadFactory);
    }

    @Test
    void constructor_throws_with_null_delegate() {
        delegateThreadFactory = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Nested
    class WithNewThread {

        @Mock
        private Thread threadFromDelegate;

        @BeforeEach
        void setUp() {
            when(delegateThreadFactory.newThread(runnable))
                    .thenReturn(threadFromDelegate);
        }

        @Test
        void newThread_returns_thread_from_inner() {
            assertThat(createObjectUnderTest().newThread(runnable),
                    equalTo(threadFromDelegate));
        }

        @Test
        void newThread_assigns_name() {
            createObjectUnderTest().newThread(runnable);
            verify(threadFromDelegate).setName("acknowledgement-callback-1");
        }
    }

    @Test
    void newThread_called_multiple_times_uses_new_thread_name() {
        when(delegateThreadFactory.newThread(runnable))
                .thenAnswer(a -> mock(Thread.class));

        final CallbackTheadFactory objectUnderTest = createObjectUnderTest();

        final Thread thread1 = objectUnderTest.newThread(runnable);
        final Thread thread2 = objectUnderTest.newThread(runnable);
        final Thread thread3 = objectUnderTest.newThread(runnable);

        verify(thread1).setName("acknowledgement-callback-1");
        verify(thread2).setName("acknowledgement-callback-2");
        verify(thread3).setName("acknowledgement-callback-3");
    }
}