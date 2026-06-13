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
	"testing"

	"github.com/MichaelSp/kswitch/types"
)

func TestBuildExecKubeconfig(t *testing.T) {
	exec := &types.ExecProvider{APIVersion: execAuthAPIVersion, Command: "aws"}
	kc := buildExecKubeconfig("ctx", "https://example.com", "CA-DATA", exec)

	if kc.TypeMeta.APIVersion != "v1" || kc.TypeMeta.Kind != "Config" {
		t.Errorf("TypeMeta = %+v, want {v1 Config}", kc.TypeMeta)
	}
	if kc.CurrentContext != "ctx" {
		t.Errorf("CurrentContext = %q, want ctx", kc.CurrentContext)
	}

	if len(kc.Clusters) != 1 {
		t.Fatalf("len(Clusters) = %d, want 1", len(kc.Clusters))
	}
	if c := kc.Clusters[0]; c.Name != "ctx" || c.Cluster.Server != "https://example.com" || c.Cluster.CertificateAuthorityData != "CA-DATA" {
		t.Errorf("cluster = %+v, want name=ctx server=https://example.com ca=CA-DATA", c)
	}

	if len(kc.Contexts) != 1 {
		t.Fatalf("len(Contexts) = %d, want 1", len(kc.Contexts))
	}
	if c := kc.Contexts[0]; c.Name != "ctx" || c.Context.Cluster != "ctx" || c.Context.User != "ctx" {
		t.Errorf("context = %+v, want all 'ctx'", c)
	}

	if len(kc.Users) != 1 {
		t.Fatalf("len(Users) = %d, want 1", len(kc.Users))
	}
	if u := kc.Users[0]; u.Name != "ctx" || u.User.ExecProvider != exec {
		t.Errorf("user = %+v, want name=ctx and the provided exec provider", u)
	}
}
