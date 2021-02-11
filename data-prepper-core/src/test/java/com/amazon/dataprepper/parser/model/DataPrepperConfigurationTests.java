package com.amazon.dataprepper.parser.model;

import java.io.File;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_DEFAULT_LOG4J_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_SOME_DEFAULT_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_DATA_PREPPER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PORT_DATA_PREPPER_CONFIG_FILE;

public class DataPrepperConfigurationTests {

    @Test
    public void testParseConfig() {
        final DataPrepperConfiguration dataPrepperConfiguration =
                DataPrepperConfiguration.fromFile(new File(VALID_DATA_PREPPER_CONFIG_FILE));
        Assert.assertEquals(5678, dataPrepperConfiguration.getServerPort());
        Assert.assertEquals(Level.DEBUG, dataPrepperConfiguration.getLog4JConfiguration().getLevel());
        Assert.assertEquals("file.txt", dataPrepperConfiguration.getLog4JConfiguration().getFilePath());
        Assert.assertEquals("1GB", dataPrepperConfiguration.getLog4JConfiguration().getMaxFileSize());
        Assert.assertEquals("10", dataPrepperConfiguration.getLog4JConfiguration().getMaxBackupIndex());
    }

    @Test
    public void testDefaultLog4jConfig() {
        final DataPrepperConfiguration dataPrepperConfiguration =
                DataPrepperConfiguration.fromFile(new File(VALID_DATA_PREPPER_DEFAULT_LOG4J_CONFIG_FILE));
        Assert.assertEquals(1234, dataPrepperConfiguration.getServerPort());
        Assert.assertEquals(Log4JConfiguration.DEFAULT_CONFIG.getLevel(), dataPrepperConfiguration.getLog4JConfiguration().getLevel());
        Assert.assertEquals(Log4JConfiguration.DEFAULT_CONFIG.getFilePath(), dataPrepperConfiguration.getLog4JConfiguration().getFilePath());
        Assert.assertEquals(Log4JConfiguration.DEFAULT_CONFIG.getMaxFileSize(), dataPrepperConfiguration.getLog4JConfiguration().getMaxFileSize());
        Assert.assertEquals(Log4JConfiguration.DEFAULT_CONFIG.getMaxBackupIndex(), dataPrepperConfiguration.getLog4JConfiguration().getMaxBackupIndex());
    }

    @Test
    public void testSomeDefaultConfig() {
        final DataPrepperConfiguration dataPrepperConfiguration =
                DataPrepperConfiguration.fromFile(new File(VALID_DATA_PREPPER_SOME_DEFAULT_CONFIG_FILE));
        Assert.assertEquals(DataPrepperConfiguration.DEFAULT_CONFIG.getServerPort(), dataPrepperConfiguration.getServerPort());
        Assert.assertEquals(Log4JConfiguration.DEFAULT_CONFIG.getLevel(), dataPrepperConfiguration.getLog4JConfiguration().getLevel());
        Assert.assertEquals("file.txt", dataPrepperConfiguration.getLog4JConfiguration().getFilePath());
        Assert.assertEquals(Log4JConfiguration.DEFAULT_CONFIG.getMaxFileSize(), dataPrepperConfiguration.getLog4JConfiguration().getMaxFileSize());
        Assert.assertEquals(Log4JConfiguration.DEFAULT_CONFIG.getMaxBackupIndex(), dataPrepperConfiguration.getLog4JConfiguration().getMaxBackupIndex());
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
