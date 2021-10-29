A default `data-prepper-config.yaml` is provided to the Docker image. This allows for Data Prepper to be run without 
supplying a `data-prepper-config.yaml` as shown in the following command.

```
docker run \
 --name data-prepper-test \
 --expose 21890 \
 -v /full/path/to/pipelines.yaml:/usr/share/data-prepper/pipelines.yaml \
 data-prepper/data-prepper:latest
```

The default [data-prepper-config.yaml](default-data-prepper-config.yaml) is copied into `/usr/share/data-prepper/data-prepper-config.yaml` in the Docker image.
The [default_keystore.p12](default-keystore.p12) is copied into `/usr/share/data-prepper/keystore.p12` in the Docker image,
and the steps that were taken to generate this keystore with `openssl`, along with its private key and self-signed certificate, are documented below.

# Step 1

Generate a private key named `default_key.key` with a password of `password`.

`openssl genrsa -des3 -passout pass:password -out default_key.key 2048 -sha256`

# Step 2 

Generate a public key named `public.pem` from the private key created in step 1.

`openssl rsa -passin pass:password -in default_key.key -outform PEM -pubout -out public.pem`

# Step 3

Generate a Certificate Signing Request named `certificate.csr` from the private key created in step 1.

`openssl req -new -passin pass:password -key default_key.key -out certificate.csr -subj "/L=test/O=Example Com Inc./OU=Example Com Inc. Root CA/CN=Example Com Inc. Root CA"`

# Step 4

Self sign the Certificate Signing Request to create a certificate named `certificate.crt`

` openssl x509 -req -days 3650 -in certificate.csr -passin pass:password -signkey default_key.key -out certificate.crt`

# Step 5

Create a `keystore.p12` with a passsword of `password` using the certificate from step 4 and the private key from step 1.

As mentioned in [configuration.md](https://github.com/opensearch-project/data-prepper/blob/main/docs/configuration.md),
the password for the private key and the keystore must be the same when using a `pkcs12` keystore.

`openssl pkcs12 -export -in certificate.crt -passin pass:password -inkey default_key.key -out default-keystore.p12 -passout pass:password`

The location of the keystore and the keystore password correspond to the following fields from the 
default `data-prepper-config.yaml`.

```yaml
keyStoreFilePath: "/usr/share/data-prepper/keystore.p12"
keyStorePassword: "password"
```

Once you have created your own `data-prepper-config.yaml` with TLS, you can use it with Data Prepper by running

```
docker run \
 --name data-prepper-test \
 -p 4900:4900 \
 --expose 21890 \
 -v /full/path/to/pipelines.yaml:/usr/share/data-prepper/pipelines.yaml \
 -v /full/path/to/data-prepper-config.yaml:/usr/share/data-prepper/data-prepper-config.yaml \
 -v /full/path/to/keystore.p12:/usr/share/data-prepper/keystore.p12 \
 data-prepper/data-prepper:latest
```