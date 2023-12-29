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

Note that `indices:admin/template/*` need to be in cluster permissions.

### Index permissions

- `Index`: `otel-v1*`; `Index permissions`: `indices_all`
- `Index`: `.opendistro-ism-config`; `Index permissions`: `indices_all`
- `Index`: `*`; `Index permission`: `manage_aliases`

`Field level security` and `Anonymization` should be left with default values.

---------------

With administrative privilege, one can create an internal user, a role and map the user to the role by following the OpenSearch [Users and roles documentation](https://opensearch.org/docs/latest/security-plugin/access-control/users-roles/).
