---
title: OVH
---

# OVH store

To use the OVH store a token should be created on OVH's website. In order to create a token you should visit https://www.ovh.com/auth/api/createToken. You will get an `application key`, `application secret` and a `consumer key`.

In order to create this token you also need to specify the scope of the application. The required permissions for this plugin to work are the following:

- `GET /cloud/project`
- `GET /cloud/project/*/kube`
- `GET /cloud/project/*/kube/*`
- `POST /cloud/project/*/kube/*/kubeconfig`

Searching over multiple OVH instances is supported, but may require `showPrefix` to be set to `true` in the `SwitchConfig` file to avoid name collisions.

## Configuration

The OVH store configuration is defined in the `kswitch` configuration file. An example configuration is shown below:

```yaml
kind: SwitchConfig
version: v1alpha1
kubeconfigStores:
- kind: ovh
  config:
    application_key: <application key>
    application_secret: <application secret>
    consumer_key: <consumer_key>
  cache:
    kind: filesystem
    config:
      path: ~/.kube/cache
```

The OVH store can be used without a filesystem cache but the OVH API will create a new Kubeconfig file (and token) every time you switch to one of the OVH contexts.
Therefore, it is recommended to use a filesystem cache.

## Authentication against the cluster

The `auth_mode` option selects how the kubeconfigs handed out by the store authenticate against the cluster. It defaults to `certificate`, so an existing configuration keeps working unchanged.

### `certificate` (default)

The kubeconfig OVH generates is used as-is. It embeds a long-lived admin client certificate, and its context is named `kubernetes-admin@<cluster name>`.

### `oidc`

The store keeps the API server address and the CA of the cluster, and replaces the admin certificate with a credential plugin, so that the cluster is reached with the identity of the user. The context is named `oidc@<cluster name>`, so the authentication a context carries is visible in its name.

Because the kubeconfig of a cluster differs per mode while its path does not, the store namespaces itself by its authentication mode: its ID becomes `ovh.<id>.oidc`, which gives the OIDC mode its own kubeconfig cache and its own search index. Switching an existing store over therefore starts from a cold cache instead of being served the admin kubeconfig cached before the switch, and switching back finds the previous one untouched.

Note that the two modes are alternatives for a given set of clusters, not a side-by-side setup: kswitch resolves the kubeconfig of a selected context by its store path, and two `kind: ovh` stores covering the same account emit the same paths (the cluster names), so the second one discovered wins for every cluster regardless of the context names. Configure one mode per account.

This requires work outside of kswitch:

- the cluster must have an OIDC provider configured, either from the OVHcloud Control Panel, from the `/cloud/project/{serviceName}/kube/{kubeId}/openIdConnect` API or with the `ovh_cloud_project_kube_oidc` Terraform resource. See [Configuring the OIDC provider on an OVHcloud Managed Kubernetes cluster](https://docs.ovhcloud.com/en/guides/public-cloud/containers-orchestration/managed-kubernetes/configure-oidc-provider). kswitch cannot detect a cluster without one: its kubeconfig is then answered with a `401`.
- the credential plugin must be installed. The defaults target [kubelogin](https://github.com/int128/kubelogin) set up as a kubectl plugin.
- an OIDC user has no permission by default. The `ClusterRoleBindings` granting them have to be created on the cluster, and an admin kubeconfig kept aside to create them.

The credentials of the store itself are unaffected: the application key, the application secret and the consumer key are what kswitch authenticates to the OVH API with, to discover the clusters and to read their endpoint.

```yaml
kind: SwitchConfig
version: v1alpha1
kubeconfigStores:
- kind: ovh
  config:
    application_key: <application key>
    application_secret: <application secret>
    consumer_key: <consumer_key>
    auth_mode: oidc
    oidc:
      # the OIDC provider the cluster trusts, e.g. a Microsoft Entra tenant
      issuer_url: https://login.microsoftonline.com/<tenant id>/v2.0
      client_id: <application (client) id>
      # requested on top of openid, to obtain the claim the cluster reads the
      # username or the groups from
      extra_scopes:
      - email
  cache:
    kind: filesystem
    config:
      path: ~/.kube/cache
```

The `oidc` section accepts these optional fields as well:

| Field | Default | Description |
| --- | --- | --- |
| `client_secret` | none | secret of a confidential OIDC client, left empty for the public clients a desktop login flow normally uses |
| `command` | `kubectl` | the credential plugin binary. Overriding it requires overriding `args` too |
| `args` | `["oidc-login", "get-token"]` | passed to `command` before the generated OIDC flags. Only defaulted when `command` is |
| `extra_args` | none | appended after the generated OIDC flags, e.g. `--grant-type=authcode-keyboard` |

The default `args` belong to the default `command`: kubelogin installed as a kubectl plugin answers to `kubectl oidc-login get-token`, the standalone binary to `kubelogin get-token`. Setting `command` without `args` is therefore rejected at startup rather than failing on the first `kubectl` call.

> **`client_secret` is not kept private.** It is passed to the credential plugin as a command line argument, so any local user can read it with `ps` while the plugin runs, and it is written verbatim into the kubeconfig cache, which kswitch creates world-readable. Prefer a public OIDC client — the usual setup for a desktop login flow, and what Microsoft Entra expects for one — and leave `client_secret` empty.

With the defaults, the generated kubeconfig invokes:

```
kubectl oidc-login get-token --oidc-issuer-url=<issuer_url> --oidc-client-id=<client_id> --oidc-extra-scope=<extra scope>...
```
