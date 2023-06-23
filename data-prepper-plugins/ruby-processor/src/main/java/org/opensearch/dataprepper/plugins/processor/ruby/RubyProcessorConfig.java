package org.opensearch.dataprepper.plugins.processor.ruby;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.util.Map;
import java.util.Objects;
import java.util.List;

public class RubyProcessorConfig {
    static final Boolean DEFAULT_SEND_MULTIPLE_EVENTS = false;
    static final Boolean DEFAULT_IGNORE_EXCEPTION = false;
    static final List<String> DEFAULT_TAGS_ON_FAILURE = List.of();

    private static final String INIT_METHOD_SIGNATURE = "def init(";
    private static final Map<String, String> DEFAULT_PARAMETERS = Map.of();
    @JsonProperty("code")
    private String code;

    @JsonProperty("path")
    private String path;

    @JsonProperty("init")
    private String initCode;

    @JsonProperty("params")
    private Map<String,String> params = DEFAULT_PARAMETERS;
    @JsonProperty("ignore_exception")
    private Boolean ignoreException = DEFAULT_IGNORE_EXCEPTION;
    @JsonProperty("tags_on_failure")
    private List<String> tagsOnFailure = DEFAULT_TAGS_ON_FAILURE; // TODO: Change this to a set.

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }

    public Boolean isCodeFromFile() {
        return Objects.nonNull(path);
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

    public Boolean isInitDefined() {
        return Objects.nonNull(initCode);
    }

    @AssertTrue(message = "exactly one of {code, path} must be specified.")
    boolean isExactlyOneOfCodeAndPathSpecified() {
        return Objects.nonNull(code) ^ Objects.nonNull(path);
    }

    @AssertTrue(message = "init must be used with code.")
    boolean isInitOnlySpecifiedWithCode() {
        return Objects.isNull(initCode) || !Objects.isNull(code); // case where init, path specified should be covered by isExactlyOneOfCodeAndPathSpecified()
    }

    @AssertTrue(message = "when ignore_exception is true, tags_on_failure must be specified.")
    boolean isTagsOnFailureSpecifiedWhenIgnoreExceptionIsTrue() {
        return !ignoreException || !tagsOnFailure.isEmpty();
    }
}