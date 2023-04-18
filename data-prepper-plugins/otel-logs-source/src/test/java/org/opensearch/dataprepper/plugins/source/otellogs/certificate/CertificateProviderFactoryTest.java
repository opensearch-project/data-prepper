/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otellogs.certificate;


import org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.core.IsInstanceOf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.acm.ACMCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.s3.S3CertificateProvider;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
public class CertificateProviderFactoryTest {
    private OTelLogsSourceConfig oTelTraceSourceConfig;
    private CertificateProviderFactory certificateProviderFactory;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void getCertificateProviderAcmProviderSuccess() {
        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("useAcmCertForSSL", true);
        settingsMap.put("awsRegion", "us-east-1");
        settingsMap.put("acmCertificateArn", "arn:aws:acm:us-east-1:account:certificate/1234-567-856456");
        settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
        settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");

        final PluginSetting pluginSetting = new PluginSetting(null, settingsMap);
        pluginSetting.setPipelineName("pipeline");
        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelLogsSourceConfig.class);

        certificateProviderFactory = new CertificateProviderFactory(oTelTraceSourceConfig);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(ACMCertificateProvider.class));
    }

    @Test
    public void getCertificateProviderS3ProviderSuccess() {
        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("ssl", true);
        settingsMap.put("awsRegion", "us-east-1");
        settingsMap.put("sslKeyCertChainFile", "s3://data/certificate/test_cert.crt");
        settingsMap.put("sslKeyFile", "s3://data/certificate/test_decrypted_key.key");

        final PluginSetting pluginSetting = new PluginSetting(null, settingsMap);
        pluginSetting.setPipelineName("pipeline");
        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelLogsSourceConfig.class);
        oTelTraceSourceConfig.validateAndInitializeCertAndKeyFileInS3();

        certificateProviderFactory = new CertificateProviderFactory(oTelTraceSourceConfig);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(S3CertificateProvider.class));
    }

    @Test
    public void getCertificateProviderFileProviderSuccess() {
        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("ssl", true);
        settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
        settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");

        final PluginSetting pluginSetting = new PluginSetting(null, settingsMap);
        pluginSetting.setPipelineName("pipeline");
        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelLogsSourceConfig.class);

        certificateProviderFactory = new CertificateProviderFactory(oTelTraceSourceConfig);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(FileCertificateProvider.class));
    }
}
