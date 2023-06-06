package org.opensearch.dataprepper.plugins.processor.ruby;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

public class RubyProcessorConfig {
    static final Boolean DEFAULT_SEND_MULTIPLE_EVENTS = false;
    static final Boolean DEFAULT_IGNORE_EXCEPTION = false;

    private static final String INIT_METHOD_SIGNATURE = "def init(";
    private Boolean initDefined; // todo: make this an optional?
    @JsonProperty("code")
    private String code;

    @JsonProperty("path")
    private String path;

    @JsonProperty("init")
    private String initCode;

    @JsonProperty("params")
    private Map<String,String> params;

    @JsonProperty("send_multiple_events")
    private Boolean sendMultipleEvents = DEFAULT_SEND_MULTIPLE_EVENTS;

    @JsonProperty("ignore_exception")
    private Boolean ignoreException = DEFAULT_IGNORE_EXCEPTION;
    public Boolean isSendMultipleEvents() {
        return sendMultipleEvents;
    }

    public Boolean isCodeFromFile() {
        return Objects.nonNull(path);
    }

    public void setSendMultipleEvents(Boolean sendMultipleEvents) {
        this.sendMultipleEvents = sendMultipleEvents;
    }

    public Boolean isIgnoreException() {
        return ignoreException;
    }

    public void setIgnoreException(Boolean ignoreException) {
        this.ignoreException = ignoreException;
    }

    public String getCode() {
        return code;
    }

    public String getPath() {
        return path;
    }

    public String getInitCode() {
        return initCode;
    }

    public Map<String, String> getParams() {
        return params;
    }

    @AssertTrue(message = "exactly one of {code, path} must be specified.")
    boolean isExactlyOneOfCodeAndPathSpecified() {
        return Objects.nonNull(code) ^ Objects.nonNull(path);
    }

    @AssertTrue(message = "init must be used with code.")
    boolean isInitSpecifiedWithCode() { // todo: rename to "OnlyWithCode"?
        return Objects.isNull(initCode) || !Objects.isNull(code); // case where init, path specified should be covered by isExactlyOneOfCodeAndPathSpecified()
    }

    @AssertTrue
    boolean isSendMultipleEventsOnlySpecifiedWithPath() {
        return sendMultipleEvents.equals(Boolean.FALSE) || !Objects.isNull(path);
    }

    @AssertTrue(message = "file path must be specified when using params")
    boolean areParamsSpecifiedWithFilePath() { // todo: rename for clarity?
        return Objects.isNull(params) || Objects.nonNull(path); // equiv to assert(params implies path)
    }
}