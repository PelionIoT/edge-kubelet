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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

const (
	FormatVersion        = "1"
	FileComplete         = "cache.current"
	FilePartial          = "cache.partial"
	CacheSaveInterval    = time.Second * 30
	InformerResyncPeriod = 0 // Don't resync
)

type LocalCache interface {
	Get(resource string, namespace string, name string, opts metav1.GetOptions) (runtime.Object, error)
	List(resource string, namespace string, opts metav1.ListOptions) (*unstructured.UnstructuredList, error)
}

type subsetCache struct {
	// immutable
	resource        string
	fieldset        fields.Set
	resourceKind    string
	initialContents bool
	initialVersion  string
	getObjDeps      DependenciesFunc
	// Mutable
	parent     *localCache
	store      cache.Store
	controller cache.Controller
	ctx        context.Context
	cancel     context.CancelFunc
	wg         *sync.WaitGroup
}

func newSubsetCache(parent *localCache, resourceInfo ResourceInfo, fieldset fields.Set, resourceVersion string, contents []unstructured.Unstructured) *subsetCache {
	getDeps := resourceInfo.GetDependencies
	if getDeps == nil {
		getDeps = func(obj runtime.Object) ([]Subset, error) {
			return []Subset{}, nil
		}
	}

	c := &subsetCache{
		resource:       resourceInfo.Name,
		resourceKind:   resourceInfo.Kind,
		fieldset:       fieldset,
		initialVersion: resourceVersion,
		getObjDeps:     getDeps,
		parent:         parent,
		wg:             &parent.wg,
	}
	c.ctx, c.cancel = context.WithCancel(parent.ctx)

	add := func(obj interface{}) {
		us := obj.(*unstructured.Unstructured)
		glog.V(8).Infof("Store Add '%v/%v' to '%v'", us.GetNamespace(), us.GetName(), c.String())
		c.updateDepsInNewThread(nil, obj)
	}

	update := func(oldObj, newObj interface{}) {
		us := newObj.(*unstructured.Unstructured)
		glog.V(8).Infof("Store Update '%v/%v' to '%v'", us.GetNamespace(), us.GetName(), c.String())
		c.updateDepsInNewThread(oldObj, newObj)
	}

	delete := func(obj interface{}) {
		us := obj.(*unstructured.Unstructured)
		glog.V(8).Infof("Store Delete '%v/%v' to '%v'", us.GetNamespace(), us.GetName(), c.String())
		c.updateDepsInNewThread(obj, nil)
	}

	lw := NewListWatcher(c.ctx, parent.clientset, resourceInfo, fieldset.AsSelector())
	store, controller := cache.NewInformer(lw, &unstructured.Unstructured{}, InformerResyncPeriod, cache.ResourceEventHandlerFuncs{
		AddFunc:    add,
		UpdateFunc: update,
		DeleteFunc: delete,
	})

	if contents != nil {

		// Add initial contents to the store
		newContents := make([]interface{}, 0, len(contents))
		for _, content := range contents {
			contentsCopy := content
			newContents = append(newContents, &contentsCopy)
		}
		store.Replace(newContents, resourceVersion)

		// Mark this cache as having initial contents so it can be used right away
		c.initialContents = true
	}

	c.store = store
	c.controller = controller

	return c
}

func (c *subsetCache) updateDepsInNewThread(oldObj, newObj interface{}) {
	oldDeps := []Subset{}
	if us, ok := oldObj.(*unstructured.Unstructured); ok {
		if deps, err := c.getObjDeps(us); err == nil {
			oldDeps = deps
		} else {
			glog.Errorf("Error getting old dependencies: '%v'", err)
		}
	}

	newDeps := []Subset{}
	if us, ok := newObj.(*unstructured.Unstructured); ok {
		if deps, err := c.getObjDeps(us); err == nil {
			newDeps = deps
		} else {
			glog.Errorf("Error getting new dependencies: '%v'", err)
		}
	}

	// Call into parent in a new go routine to prevent deadlock
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for _, dep := range newDeps {
			c.parent.addSubsetDependency(dep.Name, dep.Set, true)
		}
		for _, dep := range oldDeps {
			c.parent.removeSubsetDependency(dep.Name, dep.Set, true)
		}
	}()
}

func (c *subsetCache) list(sp storage.SelectionPredicate) (*unstructured.UnstructuredList, error) {
	ul := unstructured.UnstructuredList{
		Object: map[string]interface{}{
			"kind":       c.resourceKind + "List",
			"apiVersion": "v1",
		},
	}

	if !c.getReady() {
		return nil, fmt.Errorf("Subset cache not ready")
	}
	ul.SetResourceVersion(c.getResourceVersion())

	for _, rawObj := range c.store.List() {
		obj := rawObj.(*unstructured.Unstructured)
		if match, err := sp.Matches(obj); err != nil {
			glog.Errorf("Matching error!")
		} else if match {
			ul.Items = append(ul.Items, *obj.DeepCopy())
		}
	}

	return &ul, nil
}

func (c *subsetCache) getReady() bool {
	if c.controller.HasSynced() {
		return true
	} else if c.initialContents {
		return true
	}
	return false
}

func (c *subsetCache) getResourceVersion() string {
	if c.controller.HasSynced() {
		return c.controller.LastSyncResourceVersion()
	}
	return c.initialVersion
}

func (c *subsetCache) getSubset() Subset {
	return Subset{Name: c.resource, Set: c.fieldset}
}

func (c *subsetCache) getDeps() []Subset {
	ret := []Subset{}
	for _, obj := range c.store.List() {
		subset, err := c.getObjDeps(obj.(runtime.Object))
		if err != nil {
			glog.Errorf("Error getting deps '%v'", err)
		}

		ret = append(ret, subset...)
	}

	return ret
}

func (c *subsetCache) start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		c.controller.Run(c.ctx.Done())

		for _, dep := range c.getDeps() {
			c.parent.removeSubsetDependency(dep.Name, dep.Set, true)
		}
	}()
}

func (c *subsetCache) String() string {
	return c.resource + "/" + c.fieldset.String()
}

type localCache struct {
	mux        sync.Mutex
	wg         sync.WaitGroup
	ctx        context.Context
	caches     map[string]map[string]*subsetCache
	depsAuto   Dependencies
	depsManual Dependencies
	resources  map[string]ResourceInfo
	storeDir   string
	clientset  dynamic.Interface
	active     bool
}

// Call with SupportedResources
func NewLocalCache(ctx context.Context, storeDir string, resources []ResourceInfo, dynamicClientset dynamic.Interface) (*localCache, error) {
	lc := localCache{
		ctx:       ctx,
		clientset: dynamicClientset,
	}

	// Setup storage location
	absDir, err := filepath.Abs(storeDir)
	if err != nil {
		return nil, err
	}
	lc.storeDir = absDir

	// Verify the directory exists and that we have access to it
	checkFile := path.Join(absDir, FilePartial)
	err = ioutil.WriteFile(checkFile, []byte(""), 0600)
	if err != nil {
		return nil, fmt.Errorf("Unable to access cache dir: %v", err)
	}
	os.Remove(checkFile)

	// Setup resources
	resourceMap := map[string]ResourceInfo{}
	for _, resource := range resources {
		resourceMap[resource.Name] = resource
	}
	lc.resources = resourceMap

	// Load from disk
	caches := lc.loadCaches()
	for _, cacheType := range caches {
		for _, cache := range cacheType {
			for _, deps := range cache.getDeps() {
				lc.depsAuto.Add(deps)
			}
		}
	}

	lc.caches = caches

	return &lc, nil
}

func (c *localCache) Run() {
	if err := c.run(); err != nil {
		panic(err)
	}
	c.wg.Wait()
}

func (c *localCache) pruneCaches() {
	for done := false; !done; {
		done = true
		for _, cacheType := range c.caches {
			for set, cache := range cacheType {
				if !c.depsManual.Has(cache.getSubset()) && !c.depsAuto.Has(cache.getSubset()) {
					for _, deps := range cache.getDeps() {
						c.depsAuto.Remove(deps)
						// re-run processing since the dependencies changed
						done = false
					}
					delete(cacheType, set)
				}
			}
		}
	}
}

func (c *localCache) run() error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.active {
		return fmt.Errorf("LocalCache is already running")
	}

	// Prune caches which have not been referenced
	c.pruneCaches()

	// Add missing caches
	allDeps := append([]Subset{}, c.depsAuto.GetAll()...)
	allDeps = append(allDeps, c.depsManual.GetAll()...)
	for _, subset := range allDeps {
		resource := subset.Name
		set := subset.Set
		_, found := c.caches[resource][set.String()]
		if found {
			continue
		}

		c.caches[resource][set.String()] = newSubsetCache(c, c.resources[resource], set, "0", nil)
	}

	// Start all caches
	for _, cacheType := range c.caches {
		for _, cache := range cacheType {
			cache.start()
		}
	}

	// Mark this object as active
	c.active = true

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			ticker := time.NewTicker(CacheSaveInterval)
			defer ticker.Stop()

			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				func() {
					c.mux.Lock()
					defer c.mux.Unlock()
					c.saveCaches(c.caches)
				}()
			}
		}
	}()

	return nil
}

type localCacheClean struct {
	Version string              `json:"version"`
	Caches  []*subsetCacheClean `json:"caches"`
}

type subsetCacheClean struct {
	Resource        string                      `json:"resource"`
	Set             fields.Set                  `json:"set"`
	ResourceVersion string                      `json:"resource-version"`
	Items           []unstructured.Unstructured `json:"items"`
}

func (c *localCache) loadCaches() map[string]map[string]*subsetCache {
	caches := map[string]map[string]*subsetCache{}
	for _, resource := range c.resources {
		caches[resource.Name] = map[string]*subsetCache{}
	}

	data, err := ioutil.ReadFile(path.Join(c.storeDir, FileComplete))
	if err != nil {
		glog.Warningf("Unable to load caches from disk: '%v'", err)
		return caches
	}

	contents := localCacheClean{}
	if err := json.Unmarshal(data, &contents); err != nil {
		glog.Warningf("Cache unmarshalling failed: '%v'", err)
		return caches
	}

	if contents.Version != FormatVersion {
		glog.Warningf("Cache format '%v' is older than current '%v'. Ignoring contents", contents.Version, FormatVersion)
		return caches
	}

	glog.Infof("Loading Offline cache")
	for _, cache := range contents.Caches {
		resource, found := c.resources[cache.Resource]
		if !found {
			glog.Warningf("Unsupported resource type '%v' found in cache", cache.Resource)
			continue
		}

		sc := newSubsetCache(c, resource, cache.Set, cache.ResourceVersion, cache.Items)
		caches[cache.Resource][cache.Set.String()] = sc
		glog.Infof("  %v - %v items", sc.String(), len(cache.Items))
	}

	glog.Infof("Cache loaded successfully")
	return caches
}

func (c *localCache) saveCaches(caches map[string]map[string]*subsetCache) {
	contents := &localCacheClean{
		Version: FormatVersion,
	}

	glog.Infof("Saving Offline cache")
	for _, cacheType := range caches {
		for _, cache := range cacheType {
			if !cache.getReady() {
				// This cache hasn't loaded initial contents so skip it
				continue
			}
			version := cache.getResourceVersion()
			items := InterfaceToUnstructuredSlice(cache.store.List())

			cleanCache := &subsetCacheClean{
				Resource:        cache.resource,
				Set:             cache.fieldset,
				ResourceVersion: version,
				Items:           items,
			}
			contents.Caches = append(contents.Caches, cleanCache)
			glog.V(1).Infof("  %v - %v items", cache.String(), len(items))
		}
	}

	data, err := json.Marshal(contents)
	if err != nil {
		glog.Errorf("Could not marshal cache: '%v'", err)
		return
	}

	tmpFile := filepath.Join(c.storeDir, FilePartial)
	err = ioutil.WriteFile(tmpFile, data, 0600)
	if err != nil {
		glog.Errorf("Could not write cache: '%v'", err)
		return
	}

	completeFile := filepath.Join(c.storeDir, FileComplete)
	err = os.Rename(tmpFile, completeFile)
	if err != nil {
		glog.Errorf("Could not rename '%v' to '%v': '%v'", tmpFile, completeFile, err)
		return
	}
}

// Must be called with the lock held
func (c *localCache) errorIfUnsupported(resource string) error {
	_, ok := c.resources[resource]
	if !ok {
		return fmt.Errorf("Unsupported resource type '%s'", resource)
	}
	return nil
}

func (c *localCache) Get(resource string, namespace string, name string, opts metav1.GetOptions) (runtime.Object, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := c.errorIfUnsupported(resource); err != nil {
		return nil, err
	}

	if c.resources[resource].Namespaced && namespace == "" {
		glog.Errorf("Get missing namespace for resource '%v'", resource)
	}

	listOpts := metav1.ListOptions{
		IncludeUninitialized: opts.IncludeUninitialized,
		ResourceVersion:      opts.ResourceVersion,
		FieldSelector:        fields.Set{"metadata.name": name}.AsSelector().String(),
	}
	list, err := c.listLocked(resource, namespace, listOpts)
	if err != nil {
		return nil, fmt.Errorf("Resource '%v/%v/%v' not cached", resource, namespace, name)
	} else if len(list.Items) > 1 {
		glog.Errorf("Multiple matches for a unique object")
	} else if len(list.Items) == 1 {
		return &list.Items[0], nil
	}

	// Item is covered by cache but not found
	return nil, fmt.Errorf("Resource '%v/%v/%v' not found", resource, namespace, name)

}

func (c *localCache) List(resource string, namespace string, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := c.errorIfUnsupported(resource); err != nil {
		return nil, err
	}

	return c.listLocked(resource, namespace, opts)
}

func (c *localCache) listLocked(resource string, namespace string, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {

	fieldSelector, err := fields.ParseSelector(opts.FieldSelector)
	if err != nil {
		return nil, err
	}
	if namespace != "" {
		fieldSelector = fields.AndSelectors(
			fieldSelector,
			fields.Set{"metadata.namespace": namespace}.AsSelector(),
		)
	}

	labelSelector, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, err
	}

	sp := storage.SelectionPredicate{
		Label:                labelSelector,
		Field:                fieldSelector,
		GetAttrs:             c.resources[resource].GetAttr,
		IncludeUninitialized: opts.IncludeUninitialized,
		Limit:                opts.Limit,
		Continue:             opts.Continue,
	}

	objectIsUnique := SelectorUniquelyIdentifiesObject(sp, c.resources[resource].Namespaced)
	for _, cache := range c.caches[resource] {
		if !cache.getReady() {
			continue
		}

		// If this cache has everything the selector could match then use it
		if SetContainsSelection(cache.fieldset, sp) {
			return cache.list(sp)
		}
		glog.Infof("List '%v/%v' is not a subset of cache '%v'", resource, fieldSelector.String(), cache)

		// If there is only one object and this list has it then we know
		// this is the complete list
		if objectIsUnique {
			if list, err := cache.list(sp); err != nil {
				glog.Errorf("Call to sublist failed")
			} else if len(list.Items) > 1 {
				glog.Errorf("Multiple matches for a unique object")
			} else if len(list.Items) == 1 {
				glog.Infof("List (Unique) '%v/%v' found in cache '%v'", resource, fieldSelector, cache)
				return list, nil
			}
			glog.Infof("List (Unique) '%v/%v' is not in cache '%v'", resource, fieldSelector, cache)
		}
	}

	return nil, fmt.Errorf("Resource '%v' subgroup '%v' not cached", resource, fieldSelector)
}

// Get the number of elements stored in the cache
func (c *localCache) Count() int {
	c.mux.Lock()
	defer c.mux.Unlock()

	count := 0
	for _, cacheType := range c.caches {
		for _, cache := range cacheType {
			count += len(cache.store.List())
		}
	}
	return count
}

func (c *localCache) AddSubsetDependency(resource string, subgroup fields.Set) error {
	return c.addSubsetDependency(resource, subgroup, false)
}

func (c *localCache) addSubsetDependency(resource string, subgroup fields.Set, auto bool) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := c.errorIfUnsupported(resource); err != nil {
		return err
	}

	subset := Subset{Name: resource, Set: subgroup}
	if auto {
		c.depsAuto.Add(subset)
		if !c.active {
			glog.Errorf("Internal dependency added before activated")
		}
	} else {
		c.depsManual.Add(subset)
	}

	depType := "manual"
	if auto {
		depType = "auto"
	}
	glog.V(3).Infof("Dependency - Increment reference '%v' (auto=%v manual=%v) of '%v'", depType, c.depsAuto.Count(subset), c.depsManual.Count(subset), subset.String())

	if !c.active {
		return nil
	}

	// Create a cache if this set doesn't exist
	key := subgroup.String()
	_, ok := c.caches[resource][key]
	if !ok {
		cache := newSubsetCache(c, c.resources[resource], subgroup, "0", nil)
		c.caches[resource][key] = cache
		glog.V(3).Infof("  Adding Cache for '%v'", cache.String())
		cache.start()
	}

	return nil
}

func (c *localCache) RemoveSubsetDependency(resource string, subgroup fields.Set) error {
	return c.removeSubsetDependency(resource, subgroup, false)
}

func (c *localCache) removeSubsetDependency(resource string, subgroup fields.Set, auto bool) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := c.errorIfUnsupported(resource); err != nil {
		return err
	}
	subset := Subset{Name: resource, Set: subgroup}

	if auto {
		if !c.depsAuto.Remove(subset) {
			glog.Errorf("Subset does not exist")
			return fmt.Errorf("Subset does not exist")
		}
	} else {
		if !c.depsManual.Remove(subset) {
			return fmt.Errorf("Subset does not exist")
		}
	}

	depType := "manual"
	if auto {
		depType = "auto"
	}
	glog.V(3).Infof("Dependency - Decrement reference '%v' (auto=%v manual=%v) of '%v'", depType, c.depsAuto.Count(subset), c.depsManual.Count(subset), subset.String())

	if !c.active {
		return nil
	}

	key := subgroup.String()
	cache, ok := c.caches[resource][key]
	if !ok {
		return fmt.Errorf("Subset does not exist")
	}

	// Delete this cache if it is no longer in use
	if c.depsManual.Count(subset)+c.depsAuto.Count(subset) == 0 {
		delete(c.caches[resource], key)
		glog.V(3).Infof("  Removing Cache for '%v'", cache.String())
		cache.cancel()
	}

	return nil
}
