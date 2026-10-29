## OpenSearch


The OpenSearch sink can send trace data to an OpenSearch cluster by administrative credentials as follows:

```
sink:
  - opensearch:
      ...
      username: "admin"
      password: "<admin password>"
```

or by using user credential assigned with a role that has the below required permissions.

### Cluster permissions

- `cluster_all`
- `indices:admin/template/get`
- `indices:admin/template/put`
- `indices:admin/index_template/get`
- `indices:admin/index_template/put`
- `indices:data/write/bulk`
- `indices:data/write/bulks`

Note that `indices:admin/template/*` need to be in cluster permissions.

### Index permissions

- `Index`: `otel-v1*`; `Index permissions`: `indices_all`
- `Index`: `.opendistro-ism-config`; `Index permissions`: `indices_all`
- `Index`: `*`; `Index permission`: `manage_aliases`, `indices:admin/get`, `indices:admin/create`, `indices:data/write/index`, `indices:data/write/bulk`, `indices:data/write/bulk*`, `indices:admin/mapping/put`

`Field level security` and `Anonymization` should be left with default values.

### Amazon OpenSearch Service with fine-grained access control

On FGAC-enabled Amazon OpenSearch Service domains, the cluster and index lists above are not enough for a non-admin user. Map the sink user to a role with these permissions instead:

#### Cluster permissions

- `cluster:monitor/state`
- `indices:data/write/bulk`

#### Index permissions

- `Index`: `*`; `Index permissions`: `manage`, `write`

`Field level security` and `Anonymization` should be left with default values.

See [security.md](security.md) for IAM vs internal-user sink configuration on FGAC domains. Creating an ISM policy on Amazon OpenSearch Service is limited to the master user; for first-time Trace Analytics index setup, use the master user or a user mapped to `all_access`.

---------------

With administrative privilege, one can create an internal user, a role and map the user to the role by following the OpenSearch [Users and roles documentation](https://opensearch.org/docs/latest/security-plugin/access-control/users-roles/).
