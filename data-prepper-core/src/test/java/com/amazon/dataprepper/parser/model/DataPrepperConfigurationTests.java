/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.parser.model;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static com.amazon.dataprepper.TestDataProvider.INVALID_DATA_PREPPER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PORT_DATA_PREPPER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_SOME_DEFAULT_CONFIG_FILE;

public class DataPrepperConfigurationTests {

    @Test
    public void testParseConfig() {
        final DataPrepperConfiguration dataPrepperConfiguration =
                DataPrepperConfiguration.fromFile(new File(VALID_DATA_PREPPER_CONFIG_FILE));
        Assert.assertEquals(5678, dataPrepperConfiguration.getServerPort());
    }

    @Test
    public void testSomeDefaultConfig() {
        final DataPrepperConfiguration dataPrepperConfiguration =
                DataPrepperConfiguration.fromFile(new File(VALID_DATA_PREPPER_SOME_DEFAULT_CONFIG_FILE));
        Assert.assertEquals(DataPrepperConfiguration.DEFAULT_CONFIG.getServerPort(), dataPrepperConfiguration.getServerPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidConfig() {
            final DataPrepperConfiguration dataPrepperConfiguration =
                    DataPrepperConfiguration.fromFile(new File(INVALID_DATA_PREPPER_CONFIG_FILE));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidPortConfig() {
        final DataPrepperConfiguration dataPrepperConfiguration =
                DataPrepperConfiguration.fromFile(new File(INVALID_PORT_DATA_PREPPER_CONFIG_FILE));
    }
}
