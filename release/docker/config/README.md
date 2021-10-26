A default `data-prepper-config.yaml` is provided to the Docker image. This allows for Data Prepper to be run without 
supplying a `data-prepper-config.yaml` as shown in the following command.

```
docker run \
 --name data-prepper-test \
 --expose 21890 \
 -v /workplace/github/simple-ingest-transformation-utility-pipeline/examples/config/example-pipelines.yaml:/usr/share/data-prepper/pipelines.yaml \
 data-prepper/data-prepper:latest
```

The default `data-prepper-config.yaml` can be in `release/default-data-prepper-config.yaml`, and it looks like this

```yaml
ssl: true
keyStoreFilePath: "/usr/share/data-prepper/keystore.p12"
keyStorePassword: "password"
privateKeyPassword: "password"
serverPort: 4900
metricRegistries: [Prometheus]
```

The `default-keystore.p12` file in `release/docker` is copied into `usr/share/data-prepper/keystore.p12` in the Docker image,
and the steps that were taken to generate this keystore with `openssl`, along with its private key and self-signed certificate, are documented below.

# Step 1

Generate a private key named `default_key.key`.

`openssl genrsa -des3 -out default_key.key 2048 -sha256`

When prompted for a password, `passsword` was used. This corresponsds to the configuration field 
```yaml
privateKeyPassword: "password"
```

from the above `default-data-prepper-config.yaml` configuration.

# Step 2 

Generate a public key named `public.pem` from the private key created in step 1.

`openssl rsa -in default_key.key -outform PEM -pubout -out public.pem`

# Step 3

Generate a Certificate Signing Request named `certificate.csr` from the private key created in step 1.

`openssl req -new -key default_key.key -out certificate.csr`

The information for the Certificate Signing Request was entered as shown below

```
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) []:.
State or Province Name (full name) []:.
Locality Name (eg, city) []:test
Organization Name (eg, company) []:Example Com Inc.
Organizational Unit Name (eg, section) []:Example Com Inc. Root CA
Common Name (eg, fully qualified host name) []:Example Com Inc. Root CA
Email Address []:.

Please enter the following 'extra' attributes
to be sent with your certificate request
A challenge password []:.
```

# Step 4

Self sign the Certificate Signing Request to create a certificate named `certificate.crt`

` openssl x509 -req -days 3650 -in certificate.csr -signkey default_key.key -out certificate.crt`

# Step 5

Create a `keystore.p12` using the certificate from step 4 and the private key from step 1.

`openssl pkcs12 -export -in certificate.crt -inkey default_key.key -out default-keystore.p12`

When prompted for an export password, the password used was also `password`. As mentioned in [configuration.md](https://github.com/opensearch-project/data-prepper/blob/main/docs/configuration.md),
the password for the private key and the keystore must be the same when using a `pkcs12` keystore.

The location of the keystore and the keystore password correspond to the following fields from the 
default `data-prepper-config.yaml`.

```yaml
keyStoreFilePath: "/usr/share/data-prepper/keystore.p12"
keyStorePassword: "password"
```

Once you have created your own `data-prepper-config.yaml` with tls, you can use it with Data Prepper by running

```
docker run \
 --name data-prepper-test \
 --expose 21890 \
 -v /workplace/github/simple-ingest-transformation-utility-pipeline/examples/config/example-pipelines.yaml:/usr/share/data-prepper/pipelines.yaml \
 -v /workplace/github/simple-ingest-transformation-utility-pipeline/examples/config/example-data-prepper-config.yaml:/usr/share/data-prepper/data-prepper-config.yaml \
 data-prepper/data-prepper:1.0.0
```