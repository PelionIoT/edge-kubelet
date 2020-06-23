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

package offlinemanager

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kube-openapi/pkg/util/sets"
)

const (
	NoWatchPollInterval = time.Minute * 5

	BackoffStart = time.Second * 1
	BackoffMax   = time.Minute * 5
)

type Subset struct {
	Name string
	Set  fields.Set
}

func (d Subset) String() string {
	return d.Name + "/" + d.Set.String()
}

type depEntry struct {
	count uint32
	set   Subset
}

type Dependencies map[string]*depEntry

func (d *Dependencies) get(group Subset) *depEntry {
	if *d == nil {
		*d = map[string]*depEntry{}
	}
	return (*d)[group.String()]
}

func (d *Dependencies) Add(group Subset) {
	e := d.get(group)
	if e == nil {
		e = &depEntry{
			set:   group,
			count: 0,
		}
		(*d)[group.String()] = e
	}
	e.count++
}

func (d *Dependencies) Remove(group Subset) bool {
	e := d.get(group)
	if e == nil {
		return false
	}
	e.count--
	if e.count <= 0 {
		delete(*d, group.String())
	}
	return true
}

func (d *Dependencies) Count(group Subset) uint32 {
	m := d.get(group)
	if m == nil {
		return 0
	}
	return m.count
}

func (d *Dependencies) Has(group Subset) bool {
	m := d.get(group)
	if m == nil {
		return false
	}
	return true
}

func (d *Dependencies) GetAll() []Subset {
	ret := []Subset{}
	for _, subset := range *d {
		ret = append(ret, subset.set)
	}

	return ret
}

type DependenciesFunc func(obj runtime.Object) ([]Subset, error)

type ResourceInfo struct {
	// Resource "NAME" as returned by 'kubectl api-resources'.
	// Example: pods
	Name string
	// Resource "KIND" as returned by 'kubectl api-resources'.Same as 'kind' used in yaml files.
	// Example: Pod
	Kind            string
	Namespaced      bool
	WatchNotAllowed bool
	GetAttr         storage.AttrFunc
	GetDependencies DependenciesFunc
}

var SupportedResources []ResourceInfo = []ResourceInfo{
	{
		Kind:            "Pod",
		Name:            "pods",
		Namespaced:      true,
		GetAttr:         UnstructuredGetAttr("Pod"),
		GetDependencies: GetUnstructuredPodDeps,
	},
	{
		Name:       "nodes",
		Kind:       "Node",
		Namespaced: false,
		GetAttr:    UnstructuredGetAttr("Node"),
	},
	{
		Name:       "configmaps",
		Kind:       "ConfigMap",
		Namespaced: true,
		GetAttr:    UnstructuredGetAttr("ConfigMap"),
	},
	{
		Name:       "secrets",
		Kind:       "Secret",
		Namespaced: true,
		GetAttr:    UnstructuredGetAttr("Secret"),
	},
	{
		Name:            "persistentvolumeclaims",
		Kind:            "PersistentVolumeClaim",
		Namespaced:      true,
		WatchNotAllowed: true,
		GetAttr:         UnstructuredGetAttr("PersistentVolumeClaim"),
		GetDependencies: GetUnstructuredPVCDeps,
	},
	{
		Name:            "persistentvolumes",
		Kind:            "PersistentVolume",
		Namespaced:      false,
		WatchNotAllowed: true,
		GetAttr:         UnstructuredGetAttr("PersistentVolume"),
	},
	{
		Name:       "services",
		Kind:       "Service",
		Namespaced: true,
		GetAttr:    UnstructuredGetAttr("Service"),
	},
}

func GetUnstructuredPodDeps(obj runtime.Object) ([]Subset, error) {
	var deps []Subset

	// Check parameters
	us, ok := obj.(*unstructured.Unstructured)
	if !ok {
		glog.Error("Unsupported object type")
		return nil, fmt.Errorf("Unsupported object type '%T'", obj)
	}
	if us.GetKind() != "Pod" {
		glog.Error("Wrong object type")
		return nil, fmt.Errorf("Wrong object type '%T'", obj)
	}

	// Extract volumes
	volumes, found, err := unstructured.NestedSlice(us.Object, "spec", "volumes")
	if err != nil {
		glog.Error("Invalid object")
		return nil, err
	} else if !found {
		return []Subset{}, nil
	}

	// Create a dependency for each volume
	for _, rawVolume := range volumes {
		volume, ok := rawVolume.(map[string]interface{})
		if !ok {
			glog.Errorf("Volume type '%T' is not a map type", rawVolume)
			continue
		}

		if name, found, err := unstructured.NestedString(volume, "configMap", "name"); err != nil {
			glog.Errorf("Error retrieving volume.configMap.name")
		} else if found {
			deps = append(deps, Subset{
				Name: "configmaps",
				Set: fields.Set{
					"metadata.name":      name,
					"metadata.namespace": us.GetNamespace(),
				},
			})
		}

		if name, found, err := unstructured.NestedString(volume, "secret", "secretName"); err != nil {
			glog.Errorf("Error retrieving volume.secret.secretName")
		} else if found {
			deps = append(deps, Subset{
				Name: "secrets",
				Set: fields.Set{
					"metadata.name":      name,
					"metadata.namespace": us.GetNamespace(),
				},
			})
		}

		if name, found, err := unstructured.NestedString(volume, "persistentVolumeClaim", "claimName"); err != nil {
			glog.Errorf("Error retrieving volume.persistentVolumeClaim.claimName")
		} else if found {
			deps = append(deps, Subset{
				Name: "persistentvolumeclaims",
				Set: fields.Set{
					"metadata.name":      name,
					"metadata.namespace": us.GetNamespace(),
				},
			})
		}
	}

	for _, dep := range deps {
		glog.V(9).Infof("Resource 'pods/%v/%v' has dependency on '%v'", us.GetNamespace(), us.GetName(), dep)
	}

	return deps, nil
}

func GetUnstructuredPVCDeps(obj runtime.Object) ([]Subset, error) {

	// Check parameters
	us, ok := obj.(*unstructured.Unstructured)
	if !ok {
		glog.Error("Unsupported object type")
		return nil, fmt.Errorf("Unsupported object type '%T'", obj)
	}
	if us.GetKind() != "PersistentVolumeClaim" {
		glog.Error("Wrong object type")
		return nil, fmt.Errorf("Wrong object type '%T'", obj)
	}

	// Extract volumes
	name, found, err := unstructured.NestedString(us.Object, "spec", "volumeName")
	if err != nil {
		glog.Error("Invalid object")
		return nil, err
	} else if !found {
		return []Subset{}, nil
	}
	return []Subset{{
		Name: "persistentvolumes",
		Set:  fields.Set{"metadata.name": name},
	}}, nil

}

func UnstructuredGetAttr(resourceKind string) storage.AttrFunc {
	return func(obj runtime.Object) (labels.Set, fields.Set, bool, error) {
		us, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return nil, nil, false, fmt.Errorf("Unsupported object type '%T'", obj)
		}
		if us.GetKind() != resourceKind {
			return nil, nil, false, fmt.Errorf("Wrong object type '%T'", obj)
		}

		ls := us.GetLabels()
		fs := fields.Set{
			"metadata.name":      us.GetName(),
			"metadata.namespace": us.GetNamespace(),
		}

		// Support nodeName as a field selector for Pods
		if resourceKind == "Pod" {
			node, _, err := unstructured.NestedString(us.Object, "spec", "nodeName")
			if err != nil {
				return nil, nil, false, err
			}
			fs["spec.nodeName"] = node
		}

		// This consideres all objects initialized
		return ls, fs, false, nil
	}
}

func InterfaceToUnstructuredSlice(slice []interface{}) []unstructured.Unstructured {
	ret := make([]unstructured.Unstructured, 0, len(slice))
	for _, obj := range slice {
		ret = append(ret, *obj.(*unstructured.Unstructured))
	}
	return ret
}

// Return true if the given SelectionPredicate is a subset of the list formed by the field set
func SetContainsSelection(set fields.Set, sp storage.SelectionPredicate) bool {
	for key, value := range set {
		requiredValue, found := sp.Field.RequiresExactMatch(key)
		if !found {
			return false
		}
		if value != requiredValue {
			return false
		}
	}
	return true
}

// Return true if there is at most one object that can be matched by this selector
func SelectorUniquelyIdentifiesObject(selector storage.SelectionPredicate, namespacedResource bool) bool {
	fs := selector.Field
	_, requiresName := fs.RequiresExactMatch("metadata.name")
	if !namespacedResource && requiresName {
		return true
	}
	_, requiresNamespace := fs.RequiresExactMatch("metadata.namespace")
	if requiresName && requiresNamespace {
		return true
	}
	return false
}

// Create a new ListWatch for a reflector or other code to use
//
// To maximize compatiblity with authorizers this has the following behavior:
// - The request is always performed in the given namespace when possible.
//
//		For example when listing configmaps this is used
//    	api/v1/namespaces/default/configmaps?fieldSelector=metadata.name%3Dmy-cm%2Cmetadata.namespace%3Ddefault
//
//		rather than
//    	api/v1/configmaps?fieldSelector=metadata.name%3Dmy-cm%2Cmetadata.namespace%3Ddefault
//
//		Even though these requests select the same elements, the node authorizer in the api server
//		rejects the latter due to insufficient node premissions.
//
// - If a single item is being requested a Get is used rather than a List
//
//		For example when listing persistentvolumeclaims this is used
//			api/v1/namespaces/default/persistentvolumeclaims/my-pvc
//
//		rather than
//			api/v1/namespaces/default/persistentvolumeclaims?fieldSelector=metadata.name%3Dmy-pvc
//
//		Even though these requests select the get elements, the node authorizer in the api server
//		rejects the latter due to insufficient node premissions.
//
// Node permissions can be found in 'plugin/pkg/auth/authorizer/node/node_authorizer.go'.
//
func NewListWatcher(ctx context.Context, client dynamic.Interface, resourceInfo ResourceInfo, fieldSelector fields.Selector) *cache.ListWatch {
	grv := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: resourceInfo.Name,
	}

	name, nameSelector := fieldSelector.RequiresExactMatch("metadata.name")
	namspace, namespaceSelector := fieldSelector.RequiresExactMatch("metadata.namespace")

	var ri dynamic.ResourceInterface
	if namespaceSelector {
		ri = client.Resource(grv).Namespace(namspace)
	} else {
		ri = client.Resource(grv)
	}

	useGet := false
	if nameSelector && (!resourceInfo.Namespaced || namespaceSelector) {
		useGet = true
	}

	var listBackoff time.Duration = BackoffStart

	list := func(options metav1.ListOptions) (runtime.Object, error) {
		options.FieldSelector = fieldSelector.String()
		if useGet {
			ul := unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"kind":       resourceInfo.Kind + "List",
					"apiVersion": grv.Version,
				},
				Items: []unstructured.Unstructured{},
			}
			ul.SetResourceVersion("0")

			// Perform Get
			item, err := ri.Get(name, metav1.GetOptions{ResourceVersion: options.ResourceVersion})

			// Return an empty list if it was not found
			if errors.IsNotFound(err) {
				return &ul, nil
			}

			// Return the error if there is one
			if err != nil {
				return nil, err
			}

			// Check if the object's fields matches the selection filter
			_, fs, _, err := resourceInfo.GetAttr(item)
			if err != nil {
				return nil, err
			}
			if fieldSelector.Matches(fs) {
				ul.Items = append(ul.Items, *item)
			}
			return &ul, err
		}

		ul, err := ri.List(options)

		// Filter results (only needed for tests)
		if err != nil {
			return ul, err
		}
		newItems := make([]unstructured.Unstructured, 0, len(ul.Items))
		for _, item := range ul.Items {
			_, fs, _, err := resourceInfo.GetAttr(&item)
			if err == nil && fieldSelector.Matches(fs) {
				newItems = append(newItems, item)
			}
		}
		ul.Items = newItems

		return ul, err
	}

	listWithBackoff := func(options metav1.ListOptions) (runtime.Object, error) {
		obj, err := list(options)

		if err == nil {
			listBackoff = BackoffStart
		} else {
			glog.Warningf("Failed to list '%v'. Retrying in %v", resourceInfo.Name, listBackoff)
			SleepWithContext(ctx, listBackoff)
			listBackoff *= 2
			if listBackoff > BackoffMax {
				listBackoff = BackoffMax
			}
		}
		return obj, err
	}

	watch := func(options metav1.ListOptions) (watch.Interface, error) {
		options.FieldSelector = fieldSelector.String()
		if resourceInfo.WatchNotAllowed {
			SleepWithContext(ctx, NoWatchPollInterval)
			return nil, io.ErrUnexpectedEOF
		}

		return ri.Watch(options)
	}

	return &cache.ListWatch{
		ListFunc:  listWithBackoff,
		WatchFunc: watch,
	}
}

func SleepWithContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func ListImageTags(nodeName string, localCache LocalCache) ([]string, error) {
	unstructuredPodList, err := localCache.List("pods", "", metav1.ListOptions{FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName)})

	if err != nil {
		return nil, fmt.Errorf("could not list pods from cache: %s", err)
	}

	imageTags := sets.NewString()

	for _, pod := range unstructuredPodList.Items {
		podName, _, err := unstructured.NestedString(pod.Object, "metadata", "name")

		if err != nil {
			return nil, fmt.Errorf("Error retrieving metadata.name from pod object: %s", err)
		}

		containers, found, err := unstructured.NestedSlice(pod.Object, "spec", "containers")

		if err != nil {
			return nil, fmt.Errorf("Error retrieving spec.containers from pod object %s: %s", podName, err)
		}

		if !found {
			glog.Errorf("Pod object %s has no spec.containers field", podName)
			continue
		}

		for i, rawContainer := range containers {
			container, ok := rawContainer.(map[string]interface{})

			if !ok {
				glog.Errorf("Error retrieving spec.containers from pod object %s: %s", podName, err)
				continue
			}

			imageTag, found, err := unstructured.NestedString(container, "image")

			if err != nil {
				return nil, fmt.Errorf("Error retrieving image from pod object %s container %d: %s", podName, i, err)
			}

			if !found {
				continue
			}

			imageTags.Insert(imageTag)
		}
	}

	return imageTags.List(), nil
}
