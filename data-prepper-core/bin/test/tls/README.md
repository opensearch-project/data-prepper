# TLS for Testing

This document includes information for generating SSL certificates for testing Data Prepper.

## Generating

### Main Certificate

To generate the main certificate/key:
```
openssl req -x509 -sha256 -nodes -days 1095 -newkey rsa:2048 -subj "/L=test/O=Example Com Inc./OU=Example Com Inc. Root CA/CN=Example Com Inc. Root CA" -config data-prepper-core/src/test/resources/tls/openssl.conf -keyout data-prepper-core/src/test/resources/test-key.key -out data-prepper-core/src/test/resources/test-crt.crt
```

### Alternate Certificate

To generate the alternate name:

```
openssl req -x509 -sha256 -nodes -days 1095 -newkey rsa:2048 -subj "/L=test/O=Example Com Inc./OU=Example Com Inc. Root CA/CN=Example Com Inc. Root CA" -config data-prepper-core/src/test/resources/tls/openssl.conf -keyout data-prepper-core/src/test/resources/test-alternate-key.key -out data-prepper-core/src/test/resources/test-alternate-crt.crt
```
