// Copyright 2021 The Kswitch authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"fmt"
	"slices"
	"sync"

	"github.com/ovh/go-ovh/ovh"
	"gopkg.in/yaml.v3"

	storetypes "github.com/MichaelSp/kswitch/pkg/store/types"
	"github.com/MichaelSp/kswitch/types"
)

func init() {
	Register(types.StoreKindOVH, func(s types.KubeconfigStore, deps Dependencies) (storetypes.KubeconfigStore, error) {
		return NewOVHStore(s)
	})
}

var (
	_ storetypes.KubeconfigStore = (*OVHStore)(nil)
	_ storetypes.ContextNamer    = (*OVHStore)(nil)
)

func NewOVHStore(store types.KubeconfigStore) (*OVHStore, error) {
	ovhStoreConfig, err := ParseStoreConfig[types.StoreConfigOVH](store)
	if err != nil {
		return nil, err
	}

	ovhApplicationKey := ovhStoreConfig.OVHApplicationKey
	if len(ovhApplicationKey) == 0 {
		return nil, fmt.Errorf("when using the OVH kubeconfig store, the application key for OVH has to be provided via a SwitchConfig file")
	}
	ovhApplicationSecret := ovhStoreConfig.OVHApplicationSecret
	if len(ovhApplicationSecret) == 0 {
		return nil, fmt.Errorf("when using the OVH kubeconfig store, the application secret for OVH has to be provided via a SwitchConfig file")
	}
	ovhConsumerKey := ovhStoreConfig.OVHConsumerKey
	if len(ovhConsumerKey) == 0 {
		return nil, fmt.Errorf("when using the OVH kubeconfig store, the consumer key for OVH has to be provided via a SwitchConfig file")
	}
	ovhEndpoint := ovhStoreConfig.OVHEndpoint
	if len(ovhEndpoint) == 0 {
		ovhEndpoint = "ovh-eu"
	}

	authMode := ovhStoreConfig.OVHAuthMode
	if len(authMode) == 0 {
		authMode = types.OVHAuthModeCertificate
	}
	var oidc *types.StoreConfigOVHOIDC
	switch authMode {
	case types.OVHAuthModeCertificate:
	case types.OVHAuthModeOIDC:
		oidc, err = parseOVHOIDCConfig(ovhStoreConfig.OVHOIDC)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("the OVH kubeconfig store does not know the authentication mode %q, expected %q or %q", authMode, types.OVHAuthModeCertificate, types.OVHAuthModeOIDC)
	}

	newClient := func() (*ovh.Client, error) {
		client, err := ovh.NewClient(ovhEndpoint, ovhApplicationKey, ovhApplicationSecret, ovhConsumerKey)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize OVH client: %w", err)
		}
		return client, nil
	}

	// fail early on a malformed endpoint or credentials rather than on the first request
	if _, err := newClient(); err != nil {
		return nil, err
	}

	return &OVHStore{
		BaseStore:    NewBaseStore(types.StoreKindOVH, store),
		Clients:      newOVHClientPool(newClient),
		OVHKubeCache: newClusterCache[string, OVHKube](),
		AuthMode:     authMode,
		OIDC:         oidc,
	}, nil
}

// defaults of the OIDC credential plugin, targeting kubelogin installed as a kubectl
// plugin (the binary is then named kubectl-oidc_login and invoked as a subcommand)
const defaultOVHOIDCCommand = "kubectl"

var defaultOVHOIDCArgs = []string{"oidc-login", "get-token"}

// parseOVHOIDCConfig validates the OIDC configuration of the store and returns a copy
// with the optional fields defaulted.
func parseOVHOIDCConfig(config *types.StoreConfigOVHOIDC) (*types.StoreConfigOVHOIDC, error) {
	if config == nil {
		return nil, fmt.Errorf("when the OVH kubeconfig store authenticates with %q, the oidc configuration has to be provided via a SwitchConfig file", types.OVHAuthModeOIDC)
	}

	oidc := *config
	if len(oidc.IssuerURL) == 0 {
		return nil, fmt.Errorf("when the OVH kubeconfig store authenticates with %q, the OIDC issuer URL has to be provided via a SwitchConfig file", types.OVHAuthModeOIDC)
	}
	if len(oidc.ClientID) == 0 {
		return nil, fmt.Errorf("when the OVH kubeconfig store authenticates with %q, the OIDC client ID has to be provided via a SwitchConfig file", types.OVHAuthModeOIDC)
	}
	// the default arguments only make sense for the default command: kubelogin
	// installed as a kubectl plugin answers to "kubectl oidc-login get-token", the
	// standalone binary to "kubelogin get-token". Defaulting them independently would
	// build "kubelogin oidc-login get-token", which only fails once kubectl runs the
	// plugin, so an overridden command without arguments is rejected here instead.
	switch {
	case len(oidc.Command) == 0:
		oidc.Command = defaultOVHOIDCCommand
		if len(oidc.Args) == 0 {
			oidc.Args = slices.Clone(defaultOVHOIDCArgs)
		}
	case len(oidc.Args) == 0:
		return nil, fmt.Errorf("the OVH kubeconfig store was given the OIDC credential plugin command %q, so the arguments to invoke it with have to be provided via a SwitchConfig file as well (the default %v only applies to %q)", oidc.Command, defaultOVHOIDCArgs, defaultOVHOIDCCommand)
	}
	oidc.Args = slices.Clone(oidc.Args)
	oidc.ExtraScopes = slices.Clone(oidc.ExtraScopes)
	oidc.ExtraArgs = slices.Clone(oidc.ExtraArgs)

	return &oidc, nil
}

type OVHKube struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Project string
	// Endpoint describes how to reach the API server of the cluster. OVH only
	// discloses it inside a generated kubeconfig, so it is read out of one and
	// remembered here for the OIDC mode, which rebuilds the kubeconfig around it.
	Endpoint types.Cluster `json:"-"`
}

// GetID namespaces the store by its authentication mode. The kubeconfig of a cluster
// differs per mode while its path does not, and the caches and the search index are
// keyed by store ID: without this, a store switched to the OIDC mode would keep being
// served the admin kubeconfig cached for the certificate mode, silently reverting the
// switch. The certificate mode keeps the base ID, so existing caches stay valid.
func (r *OVHStore) GetID() string {
	if r.AuthMode == types.OVHAuthModeOIDC {
		return fmt.Sprintf("%s.%s", r.BaseStore.GetID(), types.OVHAuthModeOIDC)
	}
	return r.BaseStore.GetID()
}

const (
	// tagOVHClusterID is the search-result tag holding the unique OVH cluster ID
	tagOVHClusterID = "clusterID"
	// tagOVHProjectID is the search-result tag holding the OVH project ID
	tagOVHProjectID = "projectID"
)

func (r *OVHStore) GetContextPrefix(path string) string {
	if r.GetStoreConfig().ShowPrefix != nil && !*r.GetStoreConfig().ShowPrefix {
		return ""
	}

	if r.GetStoreConfig().ID != nil {
		return *r.GetStoreConfig().ID
	}

	return string(types.StoreKindOVH)
}

func (r *OVHStore) StartSearch(channel chan storetypes.SearchResult) {
	r.Logger.Debug("OVH: start search")

	projects := []string{}
	// list OVH projects
	err := r.Clients.get("/cloud/project", &projects)
	if err != nil {
		channel <- storetypes.SearchResult{
			KubeconfigPath: "",
			Error:          err,
		}
		return
	}

	// the OVH API only answers for one project resp. one cluster per request and each
	// round trip takes seconds. The clusters of a project are described as soon as that
	// project has been listed, so both levels are queried in parallel. A project or a
	// cluster that fails no longer aborts the search either: one inaccessible project
	// must not hide the clusters of all the others.
	clusters := make(chan ovhClusterRef)
	go func() {
		defer close(clusters)
		r.listClusters(projects, clusters, channel)
	}()
	r.describeClusters(clusters, channel)
}

// ovhClusterRef identifies a Kubernetes cluster in the OVH API.
type ovhClusterRef struct {
	project string
	id      string
}

// listClusters lists the Kubernetes clusters of every project in parallel and hands
// their references to the describers.
func (r *OVHStore) listClusters(projects []string, clusters chan<- ovhClusterRef, channel chan storetypes.SearchResult) {
	var (
		wg        sync.WaitGroup
		semaphore = make(chan struct{}, maxConcurrentListRequests)
	)

	for _, project := range projects {
		semaphore <- struct{}{}
		wg.Go(func() {
			defer func() { <-semaphore }()

			clusterIDs := []string{}
			if err := r.Clients.get(fmt.Sprintf("/cloud/project/%v/kube", project), &clusterIDs); err != nil {
				channel <- storetypes.SearchResult{
					Error: fmt.Errorf("failed to list the Kubernetes clusters of OVH project %q: %w", project, err),
				}
				return
			}

			for _, id := range clusterIDs {
				clusters <- ovhClusterRef{project: project, id: id}
			}
		})
	}
	wg.Wait()
}

// describeClusters fetches the details of the discovered clusters in parallel and
// reports them on the search channel.
func (r *OVHStore) describeClusters(clusters <-chan ovhClusterRef, channel chan storetypes.SearchResult) {
	wg := sync.WaitGroup{}

	for range maxConcurrentListRequests {
		wg.Go(func() {
			for cluster := range clusters {
				r.describeCluster(cluster, channel)
			}
		})
	}
	wg.Wait()
}

// describeCluster reports a single Kubernetes cluster on the search channel.
func (r *OVHStore) describeCluster(cluster ovhClusterRef, channel chan storetypes.SearchResult) {
	var kube OVHKube
	if err := r.Clients.get(fmt.Sprintf("/cloud/project/%v/kube/%v", cluster.project, cluster.id), &kube); err != nil {
		channel <- storetypes.SearchResult{
			Error: fmt.Errorf("failed to get the OVH Kubernetes cluster %q of project %q: %w", cluster.id, cluster.project, err),
		}
		return
	}
	kube.Project = cluster.project
	r.OVHKubeCache.Set(kube.ID, kube)

	channel <- storetypes.SearchResult{
		KubeconfigPath: kube.Name,
		// the cluster ID and project uniquely identify the cluster in the
		// OVH API. Carrying them in the tags lets the kubeconfig be fetched
		// without the in-memory cache (e.g. when a search index is used)
		// and without colliding on duplicate cluster names.
		Tags: map[string]string{
			tagOVHClusterID: kube.ID,
			tagOVHProjectID: cluster.project,
		},
		Error: nil,
	}
}

func (r *OVHStore) GetKubeconfigForPath(path string, tags map[string]string) ([]byte, error) {
	r.Logger.Debugf("OVH: getting secret for path %q", path)

	// prefer the IDs carried in the tags (set during the search): they are
	// unique and work even when the in-memory cache is empty (search index).
	clusterID := tags[tagOVHClusterID]
	project := tags[tagOVHProjectID]
	if clusterID == "" || project == "" {
		// fallback for entries without tags: resolve from the cache by name
		for _, c := range r.OVHKubeCache.Values() {
			if c.Name == path {
				clusterID = c.ID
				project = c.Project
				break
			}
		}
	}
	if clusterID == "" || project == "" {
		return nil, fmt.Errorf("could not resolve an OVH cluster ID for %q", path)
	}

	if r.AuthMode == types.OVHAuthModeOIDC {
		return r.oidcKubeconfig(path, project, clusterID)
	}

	generated, err := r.generateKubeconfig(project, clusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig for cluster '%s': %w", path, err)
	}
	return generated, nil
}

// generateKubeconfig asks OVH for a kubeconfig of the cluster. Every call issues a new
// admin client certificate, so the result is not something to fetch more than once per
// cluster.
func (r *OVHStore) generateKubeconfig(project, clusterID string) ([]byte, error) {
	response := struct {
		Content string `json:"content"`
	}{}
	if err := r.Clients.post(fmt.Sprintf("/cloud/project/%v/kube/%v/kubeconfig", project, clusterID), nil, &response); err != nil {
		return nil, err
	}
	return []byte(response.Content), nil
}

// oidcKubeconfig returns a kubeconfig reaching the cluster through the configured OIDC
// credential plugin, so that the user authenticates with their own identity instead of
// with the admin certificate OVH embeds in the kubeconfig it generates.
//
// The cluster must have an OIDC provider configured, which kswitch cannot check: an
// unconfigured cluster answers 401 to the resulting kubeconfig.
func (r *OVHStore) oidcKubeconfig(path, project, clusterID string) ([]byte, error) {
	endpoint, err := r.clusterEndpoint(path, project, clusterID)
	if err != nil {
		return nil, err
	}

	contextName := ovhOIDCContextName(path)
	kubeconfig := &types.KubeConfig{
		TypeMeta: types.TypeMeta{
			APIVersion: "v1",
			Kind:       "Config",
		},
		CurrentContext: contextName,
		Contexts: []types.KubeContext{{
			Name: contextName,
			Context: types.Context{
				Cluster: contextName,
				User:    contextName,
			},
		}},
		Clusters: []types.KubeCluster{{
			Name:    contextName,
			Cluster: endpoint,
		}},
		Users: []types.KubeUser{{
			Name: contextName,
			User: types.User{
				ExecProvider: &types.ExecProvider{
					APIVersion:  "client.authentication.k8s.io/v1beta1",
					Command:     r.OIDC.Command,
					Args:        r.oidcExecArgs(),
					InstallHint: "Install kubelogin to authenticate against this cluster with your own identity by following\nhttps://github.com/int128/kubelogin#setup",
				},
			},
		}},
	}

	return yaml.Marshal(kubeconfig)
}

// oidcExecArgs are the arguments the credential plugin is invoked with: the ones the
// configuration puts before the flags this store derives, and the ones it appends.
func (r *OVHStore) oidcExecArgs() []string {
	args := slices.Clone(r.OIDC.Args)
	args = append(args,
		fmt.Sprintf("--oidc-issuer-url=%s", r.OIDC.IssuerURL),
		fmt.Sprintf("--oidc-client-id=%s", r.OIDC.ClientID),
	)
	if len(r.OIDC.ClientSecret) > 0 {
		args = append(args, fmt.Sprintf("--oidc-client-secret=%s", r.OIDC.ClientSecret))
	}
	for _, scope := range r.OIDC.ExtraScopes {
		args = append(args, fmt.Sprintf("--oidc-extra-scope=%s", scope))
	}
	return append(args, r.OIDC.ExtraArgs...)
}

// clusterEndpoint returns how to reach the API server of the cluster. The OVH API only
// discloses it inside a generated kubeconfig, so the first call for a cluster generates
// one and the result is cached: unlike the certificate it is carved out of, the
// endpoint of a cluster does not expire.
func (r *OVHStore) clusterEndpoint(path, project, clusterID string) (types.Cluster, error) {
	if kube, ok := r.OVHKubeCache.Get(clusterID); ok && len(kube.Endpoint.Server) > 0 {
		return kube.Endpoint, nil
	}

	generated, err := r.generateKubeconfig(project, clusterID)
	if err != nil {
		return types.Cluster{}, fmt.Errorf("failed to read the endpoint of cluster '%s' out of a generated kubeconfig: %w", path, err)
	}

	endpoint, err := parseClusterEndpoint(generated)
	if err != nil {
		return types.Cluster{}, fmt.Errorf("failed to read the endpoint of cluster '%s' out of a generated kubeconfig: %w", path, err)
	}

	kube, ok := r.OVHKubeCache.Get(clusterID)
	if !ok {
		kube = OVHKube{ID: clusterID, Name: path, Project: project}
	}
	kube.Endpoint = endpoint
	r.OVHKubeCache.Set(clusterID, kube)

	return endpoint, nil
}

// parseClusterEndpoint returns how the given kubeconfig reaches the API server of its
// cluster: the address, and the trust the connection is established with.
func parseClusterEndpoint(kubeconfig []byte) (types.Cluster, error) {
	parsed := types.KubeConfig{}
	if err := yaml.Unmarshal(kubeconfig, &parsed); err != nil {
		return types.Cluster{}, fmt.Errorf("failed to parse the kubeconfig: %w", err)
	}
	if len(parsed.Clusters) == 0 {
		return types.Cluster{}, fmt.Errorf("the kubeconfig declares no cluster")
	}

	cluster := parsed.Clusters[0]
	// honour the current context when the kubeconfig holds more than one cluster
	for _, context := range parsed.Contexts {
		if context.Name != parsed.CurrentContext {
			continue
		}
		for _, candidate := range parsed.Clusters {
			if candidate.Name == context.Context.Cluster {
				cluster = candidate
			}
		}
	}

	if len(cluster.Cluster.Server) == 0 {
		return types.Cluster{}, fmt.Errorf("the kubeconfig declares no server for cluster %q", cluster.Name)
	}
	// the kubeconfig this store hands out is built from these fields alone, so a trust
	// expressed any other way (a certificate-authority file, for instance) would be
	// dropped and every request would fail on an unknown authority. Say so here rather
	// than at the first kubectl call.
	if len(cluster.Cluster.CertificateAuthorityData) == 0 && !cluster.Cluster.Insecure {
		return types.Cluster{}, fmt.Errorf("the kubeconfig declares neither an inline certificate authority nor insecure-skip-tls-verify for cluster %q", cluster.Name)
	}
	return cluster.Cluster, nil
}

// ContextNamesForPath returns the context name the kubeconfig of a cluster carries, so
// that the search does not have to generate the kubeconfig (a POST taking seconds per
// cluster) only to read that name back out of it.
func (r *OVHStore) ContextNamesForPath(path string, _ map[string]string) []string {
	if r.AuthMode == types.OVHAuthModeOIDC {
		return []string{ovhOIDCContextName(path)}
	}
	// the name OVH puts in the kubeconfigs it generates
	return []string{fmt.Sprintf("kubernetes-admin@%s", path)}
}

// ovhOIDCContextName names the context of a kubeconfig built for the OIDC mode. It
// deliberately differs from the name OVH gives its admin kubeconfigs, so that the
// authentication a context carries is visible in its name.
func ovhOIDCContextName(path string) string {
	return fmt.Sprintf("oidc@%s", path)
}
