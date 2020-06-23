/*
Copyright 2018-2020, Arm Limited and affiliates.

Licensed under the Apache License, Version 2.0 (the License);
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an AS IS BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	v1 "k8s.io/api/core/v1"
	serializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes/scheme"
	rest "k8s.io/client-go/rest"
)

type ArgusV1Interface interface {
	RESTClient() rest.Interface
	PodsGetter
	SecretsGetter
	NodesGetter
	ConfigMapsGetter
	NamespacesGetter
	PersistentVolumesGetter
	PersistentVolumeClaimsGetter
}

// ArgusV1Client is used to interact with features provided by the  group.
type ArgusV1Client struct {
	restClient rest.Interface
}

func (c *ArgusV1Client) Pods(aid, namespace string) PodInterface {
	return newPods(c, aid, namespace)
}

func (c *ArgusV1Client) Secrets(aid, namespace string) SecretInterface {
	return newSecrets(c, aid, namespace)
}

func (c *ArgusV1Client) Nodes(aid string) NodeInterface {
	return newNodes(c, aid)
}

func (c *ArgusV1Client) ConfigMaps(aid, namespace string) ConfigMapInterface {
	return newConfigMaps(c, aid, namespace)
}

func (c *ArgusV1Client) Namespaces(aid string) NamespaceInterface {
	return newNamespaces(c, aid)
}

func (c *ArgusV1Client) PersistentVolumes(aid string) PersistentVolumeInterface {
	return newPersistentVolumes(c, aid)
}

func (c *ArgusV1Client) PersistentVolumeClaims(aid, namespace string) PersistentVolumeClaimInterface {
	return newPersistentVolumeClaims(c, aid, namespace)
}

// NewForConfig creates a new ArgusV1Client for the given config.
func NewForConfig(c *rest.Config) (*ArgusV1Client, error) {
	config := *c
	if err := setConfigDefaults(&config); err != nil {
		return nil, err
	}
	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, err
	}
	return &ArgusV1Client{client}, nil
}

// NewForConfigOrDie creates a new ArgusV1Client for the given config and
// panics if there is an error in the config.
func NewForConfigOrDie(c *rest.Config) *ArgusV1Client {
	client, err := NewForConfig(c)
	if err != nil {
		panic(err)
	}
	return client
}

// New creates a new ArgusV1Client for the given RESTClient.
func New(c rest.Interface) *ArgusV1Client {
	return &ArgusV1Client{c}
}

func setConfigDefaults(config *rest.Config) error {
	gv := v1.SchemeGroupVersion
	config.GroupVersion = &gv
	config.APIPath = "/api"
	config.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: scheme.Codecs}

	if config.UserAgent == "" {
		config.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	return nil
}

// RESTClient returns a RESTClient that is used to communicate
// with API server by this client implementation.
func (c *ArgusV1Client) RESTClient() rest.Interface {
	if c == nil {
		return nil
	}
	return c.restClient
}
