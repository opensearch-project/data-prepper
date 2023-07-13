/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.loadtype.LoadTypeOptions;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class MaxMindServiceConfigTest {

    private static final String URL = "s3://mybucket10012023/GeoLite2";
    private MaxMindServiceConfig maxMindServiceConfig;

    @BeforeEach
    void setUp() {
        maxMindServiceConfig = new MaxMindServiceConfig();
    }

    @Test
    void getDatabasePathTestPositive() throws NoSuchFieldException, IllegalAccessException {
        List<DatabasePathURLConfig> databasePathURLConfigList = setDatabasePath();
        ReflectivelySetField.setField(MaxMindServiceConfig.class,
                maxMindServiceConfig, "databasePath", databasePathURLConfigList);
        assertThat(maxMindServiceConfig.getDatabasePath(), equalTo(databasePathURLConfigList));
    }

    @Test
    void getDatabasePathTestNegative() {
        assertThat(new MaxMindServiceConfig().getDatabasePath(), equalTo(null));
    }

    @Test
    void getLoadTypeTestPositive() throws NoSuchFieldException, IllegalAccessException {
        LoadTypeOptions loadTypeOptions = LoadTypeOptions.INMEMORY;
        ReflectivelySetField.setField(MaxMindServiceConfig.class,
                maxMindServiceConfig, "loadType", loadTypeOptions);
        assertThat(maxMindServiceConfig.getLoadType(), equalTo(loadTypeOptions));
    }

    @Test
    void getLoadTypeTestNegative() {
        assertThat(new MaxMindServiceConfig().getLoadType(), equalTo(null));
    }

    @Test
    void getCacheSizeTestPositive() throws NoSuchFieldException, IllegalAccessException {
        Integer cacheSize = 8192;
        ReflectivelySetField.setField(MaxMindServiceConfig.class,
                maxMindServiceConfig, "cacheSize", cacheSize);
        assertThat(maxMindServiceConfig.getCacheSize(), equalTo(cacheSize));
    }

    @Test
    void getCacheSizeTestNegative() {
        assertThat(new MaxMindServiceConfig().getCacheSize(), equalTo(null));
    }

    @Test
    void getCacheRefreshScheduleTestPositive() throws NoSuchFieldException, IllegalAccessException {
        Duration cacheRefreshSchedule = Duration.parse("PT3M");
        ReflectivelySetField.setField(MaxMindServiceConfig.class,
                maxMindServiceConfig, "cacheRefreshSchedule", cacheRefreshSchedule);
        assertThat(maxMindServiceConfig.getCacheRefreshSchedule(), equalTo(cacheRefreshSchedule));
    }

    @Test
    void getCacheRefreshScheduleTest() {
        assertThat(new MaxMindServiceConfig().getCacheRefreshSchedule(), equalTo(Duration.parse("P15D")));
    }

    private List<DatabasePathURLConfig> setDatabasePath() throws NoSuchFieldException, IllegalAccessException {

        List<DatabasePathURLConfig> databasePathURLConfigList = new ArrayList<>();
        DatabasePathURLConfig databasePathURLConfig = new DatabasePathURLConfig();
        ReflectivelySetField.setField(DatabasePathURLConfig.class, databasePathURLConfig, "url", URL);
        databasePathURLConfigList.add(databasePathURLConfig);
        return databasePathURLConfigList;
    }
}
