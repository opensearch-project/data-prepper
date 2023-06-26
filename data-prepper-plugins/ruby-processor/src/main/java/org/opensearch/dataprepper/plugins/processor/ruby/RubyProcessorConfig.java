package org.opensearch.dataprepper.plugins.processor.ruby;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.util.Map;
import java.util.Objects;
import java.util.List;

public class RubyProcessorConfig {
    static final Boolean DEFAULT_IGNORE_EXCEPTION = false;
    static final List<String> DEFAULT_TAGS_ON_FAILURE = List.of();
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
    private List<String> tagsOnFailure = DEFAULT_TAGS_ON_FAILURE;

    /**
     * A list of tags to be appended to any Event that triggers an exception.
     *
     * @return The list of tags.
     */
    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }

    /**
     * Whether the Ruby code is specified in a separate .rb file.
     * @return True if the Ruby code is specified in a separate .rb file.
     */
    public Boolean isCodeFromFile() {
        return Objects.nonNull(path);
    }

    /**
     * Whether exceptions within Ruby code should be ignored, or crash the pipeline.
     *
     * @return True if exceptions within Ruby code will be ignored, false if the pipeline will be crashed.
     */
    public Boolean isIgnoreException() {
        return ignoreException;
    }

    /**
     * The Ruby code defined with the pipeline config file with "code" option, that will be executed.
     *
     * @return The Ruby code defined with the pipeline config file that will be executed.
     */
    public String getCode() {
        return code;
    }

    /**
     * The path to the Ruby file that will be executed, relative to the root. The file must implement the interface
     * described in the README by defining `init` and `process` methods.
     *
     * @return The path to the Ruby file that will be executed.
     */
    public String getPath() {
        return path;
    }

    /**
     * Ruby code, specified in the pipeline config file with "init" option, to be executed at initialization.
     *
     * @return Ruby code, specified in the pipeline config file with "init" option, to be executed at initialization.
     */
    public String getInitCode() {
        return initCode;
    }

    /**
     * Parameters that are passed to the Ruby init method.
     *
     * @return Parameters that are passed to the Ruby init method.
     */
    public Map<String, String> getParams() {
        return params;
    }

    public Boolean isInitDefined() {
        return Objects.nonNull(initCode);
    }

    @AssertTrue(message = "exactly one of (code, path) must be specified.")
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