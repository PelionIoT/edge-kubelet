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
	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type HostnameWatcher struct {
	Clientset *kubernetes.Clientset
	Hosts     *HostnameRecords
}

func NewHostnameWatcher(restclient *restclient.Config, hostsPath, domain string) (h *HostnameWatcher, err error) {
	clientset, err := kubernetes.NewForConfig(restclient)
	if err != nil {
		return nil, err
	}

	// Returns error when the path is incorrect or the hosts file is corrupted
	hosts, err := NewHostnameRecords(hostsPath, domain)
	if err != nil {
		return nil, err
	}

	return &HostnameWatcher{Clientset: clientset, Hosts: hosts}, nil
}

// Use this method for when the kubelet is restarted and there are
// still Pods on the Node. Since we can't be sure that the `hosts`
// file still contains the Pod addresses, we must repopulate it
func (h *HostnameWatcher) podAdd(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		glog.Errorf("Failed to cast obj to *v1.Pod, this is most likely due to the informer fetching some other object than Pods: %v", obj)
		return
	}

	// Only add the Pod if it's phase is running, to avoid race conditions with deleting
	if pod.Status.Phase == v1.PodRunning {
		h.Hosts.CreateOrUpdateHostnameRecord(pod)
	}
}

func (h *HostnameWatcher) podUpdate(oldObj, newObj interface{}) {
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		glog.Errorf("Failed to cast oldObj to *v1.Pod, this is most likely due to the informer fetching some other object than Pods: %v", oldObj)
		return
	}

	newPod, ok := newObj.(*v1.Pod)
	if !ok {
		glog.Errorf("Failed to cast newObj to *v1.Pod, this is most likely due to the informer fetching some other object than Pods: %v", newObj)
		return
	}

	// Running Pod moved to terminating state, delete hostname
	if newPod.DeletionTimestamp != nil && oldPod.Status.Phase == v1.PodRunning {
		h.Hosts.DeleteHostnameRecord(newPod)
		return
	}

	// Pod goes from "Pending" => "Running" or "Running => Running" with different IP, add hostname
	if (oldPod.Status.Phase == v1.PodPending && newPod.Status.Phase == v1.PodRunning) ||
		(oldPod.Status.Phase == v1.PodRunning && newPod.Status.Phase == v1.PodRunning &&
			oldPod.Status.PodIP != newPod.Status.PodIP) {
		h.Hosts.CreateOrUpdateHostnameRecord(newPod)
	}
}

func (h *HostnameWatcher) Run(stopCh <-chan struct{}) {
	factory := informers.NewSharedInformerFactory(h.Clientset, 0)
	podInformer := factory.Core().V1().Pods().Informer()

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			h.podAdd(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			h.podUpdate(oldObj, newObj)
		},
		DeleteFunc: nil,
	})
	podInformer.Run(stopCh)
}
