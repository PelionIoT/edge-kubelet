/*
Copyright 2021, Pelion IoT and affiliates

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

package hostnameRecords

import (
	"fmt"
	"io/ioutil"

	"github.com/golang/glog"
	txeh "github.com/txn2/txeh"
	api "k8s.io/api/core/v1"
)

type HostnameRecords struct {
	Path   string
	Domain string
}

func NewHostnameRecords(hostsFilepath, domain string) (*HostnameRecords, error) {
	h := &HostnameRecords{Path: hostsFilepath, Domain: domain}
	if err := h.validate(); err != nil {
		return nil, fmt.Errorf("Unable to validate hosts file: %v", err)
	}

	return h, nil
}

func (h *HostnameRecords) validate() error {
	// Always create a new file
	err := ioutil.WriteFile(h.Path, []byte(""), 0644)
	if err != nil {
		return err
	}
	glog.V(4).Infof("Created hosts file for hostname records at: %s", h.Path)

	_, err = txeh.NewHosts(&txeh.HostsConfig{ReadFilePath: h.Path})
	if err != nil {
		glog.Errorf("Failed to create hosts object, unable to create Pod hostname records, err: %v", err)
		return err
	}

	return nil
}

func (h *HostnameRecords) CreateOrUpdateHostnameRecord(pod *api.Pod) error {
	hostname := pod.Spec.Hostname
	if hostname == "" {
		// This is not an error, as not all pods have hostname specified
		return nil
	}

	ip := pod.Status.PodIP
	if ip == "" {
		return fmt.Errorf("No IP found in Pod spec")
	}

	hosts, err := txeh.NewHosts(&txeh.HostsConfig{ReadFilePath: h.Path})
	if err != nil {
		return fmt.Errorf("Failed to access hosts file at %s", h.Path)
	}

	glog.V(4).Infof("Creating hostname record for Pod %s with IP %s)", pod.Name, ip)

	hosts.AddHost(ip, fmt.Sprintf("%s.%s", hostname, h.Domain))
	err = hosts.Save()
	if err != nil {
		glog.Errorf("Failed to write hostname and IP to hosts file for pod: %s", pod.Name)
		return err
	}

	return nil
}

func (h *HostnameRecords) DeleteHostnameRecord(pod *api.Pod) error {
	hostname := pod.Spec.Hostname
	if hostname == "" {
		// This is not an error, as not all pods have hostname specified
		return nil
	}

	hosts, err := txeh.NewHosts(&txeh.HostsConfig{ReadFilePath: h.Path})
	if err != nil {
		return fmt.Errorf("Failed to access hosts file at %s", h.Path)
	}

	glog.V(4).Infof("Removing hostname record for pod: %s", pod.Name)

	hosts.RemoveHost(fmt.Sprintf("%s.%s", hostname, h.Domain))
	err = hosts.Save()
	if err != nil {
		glog.Errorf("Failed to delete hostname and IP from hosts file for pod: %s", pod.Name)
		return err
	}

	return nil
}
