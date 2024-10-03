package org.opensearch.dataprepper.plugins.source.saas.jira.utils;

/**
 * utility class for Jira connector.
 */
public class ExceptionUtil {

  /**
   * Method to return error message based on given error code.
   *
   * @param errorCodeEnum input parameter
   * @return error message
   */
  public static String getErrorMessage(ErrorCodeEnum errorCodeEnum) {
    return "Jira Connector error code: " + errorCodeEnum.getErrorCode()
        + System.lineSeparator() + "Error message: " + errorCodeEnum.getErrorMessage();
  }

  /**
   * Method to return error message based on given error code and error message.
   *
   * @param errorCode    input parameter
   * @param errorMessage input parameter
   * @return error message
   */
  public static String getErrorMessage(final String errorCode, final String errorMessage) {
    return "Jira Connector error code: " + errorCode
        + System.lineSeparator() + "Error message: " + errorMessage;
  }
}
