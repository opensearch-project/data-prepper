package com.amazon.dataprepper.parser.model;

import java.util.Properties;
import java.util.UUID;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

public class Log4JConfigurationTests {

    @Test
    public void testLog4J() {
        final Level level = Level.INFO;
        final String filePath = UUID.randomUUID().toString();
        final String maxFileSize = UUID.randomUUID().toString();
        final String maxBackup = UUID.randomUUID().toString();

        final Log4JConfiguration log4JConfiguration = new Log4JConfiguration(
                level.toString(),
                filePath,
                maxFileSize,
                maxBackup
        );

        Assert.assertEquals(level, log4JConfiguration.getLevel());
        Assert.assertEquals(filePath, log4JConfiguration.getFilePath());
        Assert.assertEquals(maxFileSize, log4JConfiguration.getMaxFileSize());
        Assert.assertEquals(maxBackup, log4JConfiguration.getMaxBackupIndex());
    }

    @Test
    public void testProperties() {
        final Log4JConfiguration log4JConfiguration = new Log4JConfiguration(null, null, null, null);
        final Properties properties = log4JConfiguration.getProperties();
        Assert.assertEquals("ERROR, CONSOLE", properties.getProperty("log4j.rootLogger"));
        Assert.assertEquals("org.apache.log4j.ConsoleAppender", properties.getProperty("log4j.appender.CONSOLE"));
        Assert.assertEquals("org.apache.log4j.PatternLayout", properties.getProperty("log4j.appender.CONSOLE.layout"));
        Assert.assertEquals("INFO", properties.getProperty("log4j.appender.CONSOLE.Threshold"));
        Assert.assertEquals("%-4r [%t] %-5p %c %x \\u2013 %m%n", properties.getProperty("log4j.appender.CONSOLE.layout.ConversionPattern"));
        Assert.assertEquals("org.apache.log4j.RollingFileAppender", properties.getProperty("log4j.appender.file"));
        Assert.assertEquals("logs/Data-Prepper.log", properties.getProperty("log4j.appender.file.File"));
        Assert.assertEquals("10MB", properties.getProperty("log4j.appender.file.MaxFileSize"));
        Assert.assertEquals("5", properties.getProperty("log4j.appender.file.MaxBackupIndex"));
        Assert.assertEquals("INFO", properties.getProperty("log4j.appender.file.Threshold"));
        Assert.assertEquals("org.apache.log4j.PatternLayout", properties.getProperty("log4j.appender.file.layout"));
        Assert.assertEquals("%d{ISO8601} [%t] %-5p %40C - %m%n", properties.getProperty("log4j.appender.file.layout.ConversionPattern"));
        Assert.assertEquals("INFO", properties.getProperty("log4j.logger.com.amazon.dataprepper.pipeline"));
        Assert.assertEquals("INFO", properties.getProperty("log4j.logger.com.amazon.dataprepper.parser"));
    }

}
