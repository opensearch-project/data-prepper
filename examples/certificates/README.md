# Default Certificates

This document includes information for generating default SSL certificates for Data Prepper. 

## Generating

To generate the certificates:
```
openssl req -x509 -sha256 -nodes -days 1095 -newkey rsa:2048 -subj "/L=test/O=Example Com Inc./OU=Example Com Inc. Root CA/CN=Example Com Inc. Root CA" -config examples/certificates/openssl.conf -keyout examples/certificates/default_private_key.pem -out examples/certificates/default_certificate.pem
```

These certificates have to be regenerated before they expire.