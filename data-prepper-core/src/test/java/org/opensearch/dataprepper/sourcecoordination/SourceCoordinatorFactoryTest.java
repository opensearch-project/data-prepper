/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.source.SourceCoordinator;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.parser.model.sourcecoordination.DynamoDBSourceCoordinationStoreConfig;
import org.opensearch.dataprepper.parser.model.sourcecoordination.SourceCoordinationConfig;
import org.opensearch.dataprepper.parser.model.sourcecoordination.SourceCoordinationStoreConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class SourceCoordinatorFactoryTest {

    private DataPrepperConfiguration dataPrepperConfiguration;

    @BeforeEach
    void setup() {
        dataPrepperConfiguration = mock(DataPrepperConfiguration.class);
    }

    private SourceCoordinatorFactory createObjectUnderTest() {
        return new SourceCoordinatorFactory();
    }

    @Test
    void provideSourceCoordinatorWithNullSourceCoordinationConfig_returns_null() {
        given(dataPrepperConfiguration.getSourceCoordinationConfig()).willReturn(null);

        final SourceCoordinator sourceCoordinator = createObjectUnderTest().provideSourceCoordinator(dataPrepperConfiguration);

        assertThat(sourceCoordinator, nullValue());
    }

    @Test
    void provideSourceCoordinatorWithNullSourceCoordinationStoreConfig_returns_null() {
        final SourceCoordinationConfig sourceCoordinationConfig = mock(SourceCoordinationConfig.class);

        given(dataPrepperConfiguration.getSourceCoordinationConfig()).willReturn(sourceCoordinationConfig);
        given(sourceCoordinationConfig.getSourceCoordinationStoreConfig()).willReturn(null);

        final SourceCoordinator sourceCoordinator = createObjectUnderTest().provideSourceCoordinator(dataPrepperConfiguration);

        assertThat(sourceCoordinator, nullValue());
    }



    @Test
    void provideSourceCoordinatorWithNonExistentCoordinationStoreConfigType_returns_null() {
        final SourceCoordinationConfig sourceCoordinationConfig = mock(SourceCoordinationConfig.class);
        final SourceCoordinationStoreConfig sourceCoordinationStoreConfig = mock(SourceCoordinationStoreConfig.class);

        given(dataPrepperConfiguration.getSourceCoordinationConfig()).willReturn(sourceCoordinationConfig);
        given(sourceCoordinationConfig.getSourceCoordinationStoreConfig()).willReturn(sourceCoordinationStoreConfig);

        final SourceCoordinator sourceCoordinator = createObjectUnderTest().provideSourceCoordinator(dataPrepperConfiguration);

        assertThat(sourceCoordinator, nullValue());
    }

    @Test
    void provideSourceCoordinatorWithDynamoDBSourceCoordinationStoreConfig_returns_expected_DynamoDbSourceCoordinator() {
        final SourceCoordinationConfig sourceCoordinationConfig = mock(SourceCoordinationConfig.class);
        final SourceCoordinationStoreConfig sourceCoordinationStoreConfig = mock(DynamoDBSourceCoordinationStoreConfig.class);

        given(dataPrepperConfiguration.getSourceCoordinationConfig()).willReturn(sourceCoordinationConfig);
        given(sourceCoordinationConfig.getSourceCoordinationStoreConfig()).willReturn(sourceCoordinationStoreConfig);


        final SourceCoordinator sourceCoordinator = createObjectUnderTest().provideSourceCoordinator(dataPrepperConfiguration);

        assertThat(sourceCoordinator, notNullValue());
        assertThat(sourceCoordinator, instanceOf(DynamoDbSourceCoordinator.class));
    }
}
