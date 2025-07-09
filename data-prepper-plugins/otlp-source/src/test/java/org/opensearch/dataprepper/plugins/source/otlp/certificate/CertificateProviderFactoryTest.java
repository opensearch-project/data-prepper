/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otlp.certificate;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.core.IsInstanceOf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.acm.ACMCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.s3.S3CertificateProvider;
import org.opensearch.dataprepper.plugins.source.otlp.OTLPSourceConfig;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
public class CertificateProviderFactoryTest {
  private OTLPSourceConfig otlpSourceConfig;
  private CertificateProviderFactory certificateProviderFactory;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void getCertificateProviderAcmProviderSuccess() {
    final Map<String, Object> settingsMap = new HashMap<>();
    settingsMap.put("use_acm_certificate_for_ssl", true);
    settingsMap.put("aws_region", "us-east-1");
    settingsMap.put("acm_certificate_arn", "arn:aws:acm:us-east-1:account:certificate/1234-567-856456");
    settingsMap.put("ssl_certificate_file", "src/test/resources/certificate/test_cert.crt");
    settingsMap.put("ssl_key_file", "src/test/resources/certificate/test_decrypted_key.key");

    final PluginSetting pluginSetting = new PluginSetting(null, settingsMap);
    pluginSetting.setPipelineName("pipeline");
    otlpSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
        OTLPSourceConfig.class);

    certificateProviderFactory = new CertificateProviderFactory(otlpSourceConfig);
    final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

    assertThat(certificateProvider, IsInstanceOf.instanceOf(ACMCertificateProvider.class));
  }

  @Test
  public void getCertificateProviderS3ProviderSuccess() {
    final Map<String, Object> settingsMap = new HashMap<>();
    settingsMap.put("ssl", true);
    settingsMap.put("aws_region", "us-east-1");
    settingsMap.put("ssl_certificate_file", "s3://src/test/resources/certificate/test_cert.crt");
    settingsMap.put("ssl_key_file", "s3://src/test/resources/certificate/test_decrypted_key.key");

    final PluginSetting pluginSetting = new PluginSetting(null, settingsMap);
    pluginSetting.setPipelineName("pipeline");
    otlpSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
        OTLPSourceConfig.class);
    otlpSourceConfig.validateAndInitializeCertAndKeyFileInS3();

    certificateProviderFactory = new CertificateProviderFactory(otlpSourceConfig);
    final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

    assertThat(certificateProvider, IsInstanceOf.instanceOf(S3CertificateProvider.class));
  }

  @Test
  public void getCertificateProviderFileProviderSuccess() {
    final Map<String, Object> settingsMap = new HashMap<>();
    settingsMap.put("ssl", true);
    settingsMap.put("ssl_certificate_file", "src/test/resources/certificate/test_cert.crt");
    settingsMap.put("ssl_key_file", "src/test/resources/certificate/test_decrypted_key.key");

    final PluginSetting pluginSetting = new PluginSetting(null, settingsMap);
    pluginSetting.setPipelineName("pipeline");
    otlpSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
        OTLPSourceConfig.class);

    certificateProviderFactory = new CertificateProviderFactory(otlpSourceConfig);
    final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

    assertThat(certificateProvider, IsInstanceOf.instanceOf(FileCertificateProvider.class));
  }
}
