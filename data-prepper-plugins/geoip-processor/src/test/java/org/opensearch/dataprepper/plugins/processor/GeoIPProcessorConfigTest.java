/*
 * Copyright OpenSearch Contributors
 *  PDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.configuration.KeysConfig;
import org.opensearch.dataprepper.plugins.processor.configuration.ServiceTypeOptions;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class GeoIPProcessorConfigTest {

    private GeoIPProcessorConfig geoIPProcessorConfig;

    @BeforeEach
    void setUp() {
        geoIPProcessorConfig = new GeoIPProcessorConfig();
    }

    @Test
    void getAwsAuthenticationOptionsTestNegative() {
        assertThat(new GeoIPProcessorConfig().getAwsAuthenticationOptions(), equalTo(null));
    }

    @Test
    void getKeysConfigTestPositive() throws NoSuchFieldException, IllegalAccessException {
        List<KeysConfig> keysConfigList = new ArrayList<>();
        keysConfigList.add(new KeysConfig());
        ReflectivelySetField.setField(GeoIPProcessorConfig.class,
                geoIPProcessorConfig, "keysConfig", keysConfigList);
        assertThat(geoIPProcessorConfig.getKeysConfig(), equalTo(keysConfigList));

    }

    @Test
    void getKeysConfigTestNegative() {
        assertThat(new GeoIPProcessorConfig().getKeysConfig(), equalTo(null));
    }


    @Test
    void getServiceTypeTestPositive() throws NoSuchFieldException, IllegalAccessException {
        ServiceTypeOptions serviceTypeOptions = new ServiceTypeOptions();
        ReflectivelySetField.setField(GeoIPProcessorConfig.class,
                geoIPProcessorConfig, "serviceType", serviceTypeOptions);
        assertThat(geoIPProcessorConfig.getServiceType(), equalTo(serviceTypeOptions));
    }

    @Test
    void getServiceTypeNegative() {
        assertThat(new GeoIPProcessorConfig().getServiceType(), equalTo(null));
    }

}
