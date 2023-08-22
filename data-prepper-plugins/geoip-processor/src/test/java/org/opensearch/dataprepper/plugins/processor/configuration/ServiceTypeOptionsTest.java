/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class ServiceTypeOptionsTest {

    private ServiceTypeOptions serviceTypeOptions;

    @BeforeEach
    void setUp() {
        serviceTypeOptions = new ServiceTypeOptions();
    }

    @Test
    void getMaxMindServiceTestPositive() throws NoSuchFieldException, IllegalAccessException {
        MaxMindServiceConfig maxMindServiceConfig = new MaxMindServiceConfig();
        ReflectivelySetField.setField(ServiceTypeOptions.class,
                serviceTypeOptions, "maxMindService", maxMindServiceConfig);
        assertThat(serviceTypeOptions.getMaxMindService(), equalTo(maxMindServiceConfig));
    }

    @Test
    void getMaxMindServiceTestNegative() {
        assertThat(new ServiceTypeOptions().getMaxMindService(), equalTo(null));
    }
}
