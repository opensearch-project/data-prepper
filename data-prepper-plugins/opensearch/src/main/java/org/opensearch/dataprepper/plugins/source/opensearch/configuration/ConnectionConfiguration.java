/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import org.opensearch.dataprepper.plugins.certificate.validation.PemObjectValidator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

public class ConnectionConfiguration {

  @Deprecated
  @JsonProperty("cert")
  private Path certPath;

  @JsonAlias("certiciate_content")
  @JsonProperty("certificate")
  private String certificate;

  @JsonProperty("socket_timeout")
  private Duration socketTimeout;

  @JsonProperty("connection_timeout")
  private Duration connectTimeout;

  @JsonProperty("insecure")
  private boolean insecure = false;

  public Path getCertPath() {
    return certPath;
  }

  public String getCertificate() {
    return certificate;
  }

  public Duration getSocketTimeout() {
    return socketTimeout;
  }

  public Duration getConnectTimeout() {
    return connectTimeout;
  }

  public boolean isInsecure() {
    return insecure;
  }

  @AssertTrue(message = "cert and certificate both are configured. " +
          "Please use only one configuration.")
  boolean certificateFileAndContentAreMutuallyExclusive() {
    if(certPath == null && certificate == null)
      return true;
    return certPath != null ^ certificate != null;
  }

  @AssertTrue(message = "certificate must be either valid PEM file path or public key content.")
  boolean isCertificateValid() {
    if (PemObjectValidator.isPemObject(certificate)) {
      return true;
    }
    try {
      Paths.get(certificate);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
