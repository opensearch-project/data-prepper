package org.opensearch.dataprepper.plugins.source.saas.jira.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExceptionUtilTest {

    String errorCode;
    String errorMessage;

    private ExceptionUtil exceptionUtil;

    @BeforeEach
    void setUp() {
        errorCode = "123";
        errorMessage = "Test Error Message";
    }

    @Test
    void testInitialization() {
        exceptionUtil = new ExceptionUtil();
        assertNotNull(exceptionUtil);
    }

    @Test
    void errorCodeEnumHandlingTest(){
        assertTrue(ExceptionUtil.getErrorMessage(ErrorCodeEnum.FIELD_SIZE_OVER_MAX_LIMIT).contains("Field Size is more than max limit"));
        assertTrue(ExceptionUtil.getErrorMessage(ErrorCodeEnum.ERROR_JIRA_PROJECT_KEY_FILTER).contains("IRA Project Key Filter list size is too large"));
    }

    @Test
    void errorMessageHandlingTest(){
        assertTrue(ExceptionUtil.getErrorMessage(errorCode, errorMessage).contains(errorCode));
        assertTrue(ExceptionUtil.getErrorMessage(errorCode, errorMessage).contains(errorMessage));
    }
}
