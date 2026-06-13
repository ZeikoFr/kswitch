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

import "github.com/MichaelSp/kswitch/types"

// execAuthAPIVersion is the client-go exec-credential plugin API version used
// by the cloud stores that delegate authentication to an external binary.
const execAuthAPIVersion = "client.authentication.k8s.io/v1beta1"

// buildExecKubeconfig assembles a single-cluster kubeconfig whose user
// authenticates through the given exec credential plugin. contextName is used
// as the cluster, context and user name. It factors out the identical
// scaffolding the EKS and GKE stores build before marshalling.
func buildExecKubeconfig(contextName, server, caData string, exec *types.ExecProvider) *types.KubeConfig {
	return &types.KubeConfig{
		TypeMeta: types.TypeMeta{
			APIVersion: "v1",
			Kind:       "Config",
		},
		Clusters: []types.KubeCluster{{
			Name: contextName,
			Cluster: types.Cluster{
				CertificateAuthorityData: caData,
				Server:                   server,
			},
		}},
		CurrentContext: contextName,
		Contexts: []types.KubeContext{{
			Name: contextName,
			Context: types.Context{
				Cluster: contextName,
				User:    contextName,
			},
		}},
		Users: []types.KubeUser{{
			Name: contextName,
			User: types.User{ExecProvider: exec},
		}},
	}
}
