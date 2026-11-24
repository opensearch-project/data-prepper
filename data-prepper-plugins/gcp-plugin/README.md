# GCP Plugin

Provides `GcpCredentialsSupplier` (from `gcp-plugin-api`) to Data Prepper plugins that use Google Cloud,
the same way `aws-plugin` provides `AwsCredentialsSupplier`.

## Using it from a plugin

Add a `GcpConfig` field (the shared `gcp:` block) to the plugin's configuration, and inject `GcpCredentialsSupplier`
through the `@DataPrepperPluginConstructor`:

```java
final GoogleCredentials credentials = gcpCredentialsSupplier.getCredentials(config.getGcpConfig().toCredentialsOptions());
```

Call `getCredentials` in your plugin constructor so malformed configuration fails at startup. Building credentials does
not fetch a token, so authentication failures (missing permissions, an unreachable token endpoint) surface on first use.

## Configuration

Extension defaults, in `data-prepper-config.yaml`:

```yaml
extensions:
  gcp:
    configurations:
      default:
        project_id: my-project
        auth_mode: adc
```

A plugin's `gcp:` block:

```yaml
gcp:
  project_id: my-project
  auth_mode: service_account_key       # or adc
  client_email: sa@my-project.iam.gserviceaccount.com
  private_key: ${{aws_secrets:sa-key:private_key}}
  token_uri: https://oauth2.googleapis.com/token   # optional
  scopes: https://www.googleapis.com/auth/cloud-platform   # optional, list or single value
```

* A plugin gets the credentials from its own `gcp:` block if that sets `auth_mode`. Otherwise it gets
  `extensions.gcp.configurations.default` if that sets `auth_mode`. Otherwise it gets Application Default Credentials (ADC).
* `adc` covers `GOOGLE_APPLICATION_CREDENTIALS` (including Workload Identity Federation and
  `impersonated_service_account` files), gcloud user credentials, and the metadata server (GKE Workload Identity, GCE, Cloud Run).
* `token_uri` must be `https://oauth2.googleapis.com` on the default port. This guard comes from the ml-commons
  `google_cloud` connector, and stops the signed JWT from being sent anywhere else.
* Values under `extensions.gcp.configurations` cannot use `${{...}}` references. A plugin's own `gcp:` block can.
  Using `service_account_key` in the extension default therefore means a plaintext private key in
  `data-prepper-config.yaml`. Prefer `adc` there, with `GOOGLE_APPLICATION_CREDENTIALS` or Workload Identity.
* A plugin's `scopes` and `project_id` are kept when its credentials come from the extension default or ADC.
* `getDefaultProjectId()` returns the extension default's `project_id`. If none is set, it returns the ADC project, except
  when the extension default uses `service_account_key`, where you should set `project_id` explicitly.
  The ADC project is looked up once and cached until restart, including when ADC is unavailable.
* A `service_account_key` extension default is built at startup, so a bad key or `token_uri` fails fast.
* `token_uri` is only valid with `service_account_key`, and must not contain user info.
* Named configurations are not supported yet. Only `default` is accepted.
