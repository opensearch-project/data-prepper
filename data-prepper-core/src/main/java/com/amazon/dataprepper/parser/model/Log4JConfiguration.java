package com.amazon.dataprepper.parser.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.log4j.Level;

public class Log4JConfiguration {
    private Level level = Level.ERROR;
    private String filePath = "logs/Data-Prepper.log";
    private String maxFileSize = "10MB";
    private String maxBackupIndex = "5";
    //TODO: Support log level by plugin

    public static final Log4JConfiguration DEFAULT_CONFIG = new Log4JConfiguration();

    private Log4JConfiguration() {

    }

    @JsonCreator
    public Log4JConfiguration(
        @JsonProperty("logLevel") final String logLevel,
        @JsonProperty("filePath") final String filePath,
        @JsonProperty("maxFileSize") final String maxFileSize,
        @JsonProperty("maxBackupIndex") final String maxBackupIndex
    ) {
        setLevel(logLevel);
        setFilePath(filePath);
        setMaxFileSize(maxFileSize);
        setMaxBackupIndex(maxBackupIndex);
    }

    private void setLevel(String level) {
        if(level != null && !level.isEmpty()) {
            this.level = Level.toLevel(level);
        }
    }

    private void setFilePath(String filePath) {
        if(filePath != null && !filePath.isEmpty()) {
            this.filePath = filePath;
        }
    }

    private void setMaxFileSize(String maxFileSize) {
        if(maxFileSize != null && !maxFileSize.isEmpty()) {
            this.maxFileSize = maxFileSize;
        }
    }

    private void setMaxBackupIndex(String maxBackupIndex) {
        if(maxBackupIndex != null && !maxBackupIndex.isEmpty()) {
            this.maxBackupIndex = maxBackupIndex;
        }
    }

    public Level getLevel() {
        return level;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getMaxFileSize() {
        return maxFileSize;
    }

    public String getMaxBackupIndex() {
        return maxBackupIndex;
    }

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.putAll(PRESET_PROPERTIES);
        properties.put(Log4JPropertyKeys.ROOT_LOGGER, level.toString() + ", CONSOLE");
        properties.put(Log4JPropertyKeys.FILE, filePath);
        properties.put(Log4JPropertyKeys.FILE_MAX_SIZE, maxFileSize);
        properties.put(Log4JPropertyKeys.FILE_MAX_BACKUP_INDEX, maxBackupIndex);
        return properties;
    }

    private static Map<String, String> PRESET_PROPERTIES = new HashMap<String, String>(){{
        put(Log4JPropertyKeys.CONSOLE_APPENDER, "org.apache.log4j.ConsoleAppender");
        put(Log4JPropertyKeys.CONSOLE_LAYOUT, "org.apache.log4j.PatternLayout");
        put(Log4JPropertyKeys.CONSOLE_THRESHOLD, "INFO");
        put(Log4JPropertyKeys.CONSOLE_CONVERSION_PATTERN, "%-4r [%t] %-5p %c %x \\u2013 %m%n");
        put(Log4JPropertyKeys.FILE_APPENDER, "org.apache.log4j.RollingFileAppender");
        put(Log4JPropertyKeys.FILE_THRESHOLD, "INFO");
        put(Log4JPropertyKeys.FILE_LAYOUT, "org.apache.log4j.PatternLayout");
        put(Log4JPropertyKeys.FILE_LAYOUT_CONVERSION_PATTERN, "%d{ISO8601} [%t] %-5p %40C - %m%n");
        put(Log4JPropertyKeys.PIPELINE_LOG_LEVEL, "INFO");
        put(Log4JPropertyKeys.PARSER_LOG_LEVEL, "INFO");
    }};

    private static class Log4JPropertyKeys {
        public static String ROOT_LOGGER = "log4j.rootLogger";
        public static String CONSOLE_APPENDER = "log4j.appender.CONSOLE";
        public static String CONSOLE_LAYOUT = "log4j.appender.CONSOLE.layout";
        public static String CONSOLE_THRESHOLD = "log4j.appender.CONSOLE.Threshold";
        public static String CONSOLE_CONVERSION_PATTERN = "log4j.appender.CONSOLE.layout.ConversionPattern";
        public static String FILE_APPENDER = "log4j.appender.file";
        public static String FILE = "log4j.appender.file.File";
        public static String FILE_MAX_SIZE = "log4j.appender.file.MaxFileSize";
        public static String FILE_MAX_BACKUP_INDEX = "log4j.appender.file.MaxBackupIndex";
        public static String FILE_THRESHOLD = "log4j.appender.file.Threshold";
        public static String FILE_LAYOUT = "log4j.appender.file.layout";
        public static String FILE_LAYOUT_CONVERSION_PATTERN = "log4j.appender.file.layout.ConversionPattern";
        public static String PIPELINE_LOG_LEVEL = "log4j.logger.com.amazon.dataprepper.pipeline";
        public static String PARSER_LOG_LEVEL = "log4j.logger.com.amazon.dataprepper.parser";
    }

}
