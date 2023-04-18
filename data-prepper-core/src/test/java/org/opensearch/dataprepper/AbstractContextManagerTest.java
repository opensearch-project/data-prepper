/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractContextManagerTest {
    private List<AnnotationConfigApplicationContext> applicationContexts;
    private DataPrepper dataPrepper;

    @BeforeEach
    void setUp() {
        applicationContexts = new ArrayList<>();

        dataPrepper = mock(DataPrepper.class);
    }

    private AbstractContextManager createObjectUnderTest() {
        try(final MockedConstruction<AnnotationConfigApplicationContext> mockConstruction = mockConstruction(AnnotationConfigApplicationContext.class)) {
            final AbstractContextManager objectUnderTest = spy(AbstractContextManager.class);
            applicationContexts.addAll(mockConstruction.constructed());
            return objectUnderTest;
        }
    }

    @Test
    void getDataPrepperBean_calls_pre_refresh_hooks() {
        final AbstractContextManager objectUnderTest = createObjectUnderTest();

        assertThat(applicationContexts, notNullValue());

        final AnnotationConfigApplicationContext coreContext = applicationContexts.get(1);
        when(coreContext.getBean(DataPrepper.class)).thenReturn(dataPrepper);

        verify(objectUnderTest, never()).preRefreshPublicApplicationContext(any());
        verify(objectUnderTest, never()).preRefreshCoreApplicationContext(any());

        objectUnderTest.getDataPrepperBean();

        verify(objectUnderTest).preRefreshPublicApplicationContext(applicationContexts.get(0));
        verify(objectUnderTest).preRefreshCoreApplicationContext(applicationContexts.get(1));
    }

    @Test
    void getDataPrepperBean_calls_refresh_once_across_multiple_calls() {
        final AbstractContextManager objectUnderTest = createObjectUnderTest();

        assertThat(applicationContexts, notNullValue());

        final AnnotationConfigApplicationContext coreContext = applicationContexts.get(1);
        when(coreContext.getBean(DataPrepper.class)).thenReturn(dataPrepper);

        for (int i = 0; i < 3; i++) {
            objectUnderTest.getDataPrepperBean();
        }

        verify(applicationContexts.get(0)).refresh();
        verify(applicationContexts.get(1)).refresh();
    }

    @Test
    void getDataPrepperBean_closes_application_contexts_when_DataPrepper_shutsdown() {
        final AbstractContextManager objectUnderTest = createObjectUnderTest();

        assertThat(applicationContexts, notNullValue());

        final AnnotationConfigApplicationContext coreContext = applicationContexts.get(1);
        when(coreContext.getBean(DataPrepper.class)).thenReturn(dataPrepper);

        objectUnderTest.getDataPrepperBean();

        ArgumentCaptor<DataPrepperShutdownListener> dataPrepperShutdownListenerArgumentCaptor = ArgumentCaptor.forClass(DataPrepperShutdownListener.class);
        verify(dataPrepper).registerShutdownHandler(dataPrepperShutdownListenerArgumentCaptor.capture());

        final DataPrepperShutdownListener shutdownListener = dataPrepperShutdownListenerArgumentCaptor.getValue();

        shutdownListener.handleShutdown();

        final InOrder inOrder = inOrder(applicationContexts.toArray());
        final List<AnnotationConfigApplicationContext> reversedContexts = Lists.reverse(applicationContexts);
        for (AnnotationConfigApplicationContext applicationContext : reversedContexts) {
            inOrder.verify(applicationContext).close();
        }
    }
}