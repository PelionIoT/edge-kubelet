/*
Copyright 2018-2020, Arm Limited and affiliates.
Copyright 2021, Pelion IoT and affiliates.

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
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const (
	// Wait a little bit before closing watches so they are not immediately retried by Kubelet
	KubeletWatchTimeout  = time.Second * 30
	XRoutingBlockHeader  = "X-Routing-Block"
	XForwardedHostHeader = "X-Forwarded-Host"
	XOriginHostHeader    = "X-Origin-Host"
)

type roundTripperFilter struct {
	parent http.RoundTripper
}

type CachingServer struct {
	ctx    context.Context
	cache  *localCache
	server http.Server
}

func (rtf *roundTripperFilter) RoundTrip(r *http.Request) (*http.Response, error) {
	if err, ok := r.Header[XRoutingBlockHeader]; ok {
		glog.V(4).Infof("Blocked request to API Server: %s", r.URL.Path)
		return nil, fmt.Errorf("%s: %s", XRoutingBlockHeader, err)
	}
	return rtf.parent.RoundTrip(r)
}

func NewCachingServer(ctx context.Context, node string, storeDir string, config *rest.Config) (*CachingServer, error) {
	c := &CachingServer{
		ctx: ctx,
	}

	// Setup the cache directory
	absDir, err := filepath.Abs(storeDir)
	if err != nil {
		return nil, err
	}
	os.MkdirAll(absDir, os.ModePerm)

	// Setup the clientset used to request resources
	clientset, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	// Setup the cache controller
	cache, err := NewLocalCache(context.Background(), absDir, SupportedResources, clientset)
	if err != nil {
		return nil, err
	}
	cache.AddSubsetDependency("pods", fields.Set{"spec.nodeName": node})
	cache.AddSubsetDependency("nodes", fields.Set{"metadata.name": node})
	cache.AddSubsetDependency("services", fields.Set{})
	cache.AddSubsetDependency("endpoints", fields.Set{})
	cache.AddSubsetDependency("namespaces", fields.Set{})
	cache.AddSubsetDependency("networkpolicies", fields.Set{})
	c.cache = cache

	// Setup routes to handle
	wsContainer := restful.NewContainer()
	wsContainer.ServiceErrorHandler(func(err restful.ServiceError, r *restful.Request, w *restful.Response) {
		w.WriteHeader(http.StatusBadGateway)
	})
	ws := new(restful.WebService)
	ws.Path("/")
	ws.Route(ws.GET("/api/v1/{resource}").To(c.list))
	ws.Route(ws.GET("/api/v1/{resource}/{name}").To(c.get))
	ws.Route(ws.GET("/api/v1/namespaces/{namespace}/{resource}").To(c.list))
	ws.Route(ws.GET("/api/v1/namespaces/{namespace}/{resource}/{name}").To(c.get))
	ws.Route(ws.GET("/api/v1/namespaces/{namespace}/{resource}/{name}/status").To(c.status))
	ws.Route(ws.GET("/apis/networking.k8s.io/v1/{resource}").To(c.list))
	ws.Route(ws.GET("/apis/networking.k8s.io/v1/{resource}/{name}").To(c.get))
	ws.Route(ws.GET("/apis/networking.k8s.io/v1/namespaces/{namespace}/{resource}").To(c.list))
	ws.Route(ws.GET("/apis/networking.k8s.io/v1/namespaces/{namespace}/{resource}/{name}").To(c.get))
	ws.Route(ws.GET("/apis/networking.k8s.io/v1/namespaces/{namespace}/{resource}/{name}/status").To(c.status))
	wsContainer.Add(ws)

	// Setup the proxy. This will send requests to the same server
	// the above clientset is using. Any requests which timeout
	// will fallback to the cached version.
	destUrl, err := url.Parse(config.Host)
	if err != nil {
		return nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(destUrl)

	proxy.Transport = &roundTripperFilter{
		parent: http.DefaultTransport,
	}

	// Modify outgoing query to only fetch resources related to the Node.
	// Since we don't support communication between nodes, other nodes are
	// irrelevant.
	proxy.Director = func(req *http.Request) {
		req.Header.Add(XForwardedHostHeader, req.Host)
		req.Header.Add(XForwardedHostHeader, destUrl.Host)
		req.URL.Scheme = destUrl.Scheme
		req.URL.Host = destUrl.Host

		// Match both global and namespaced Pod requests
		matched, err := regexp.MatchString(`^/api/v1/(namespaces/.*/pods)|pods$`, req.URL.Path)
		if err != nil {
			return
		}
		if matched {
			if !strings.Contains(req.URL.RawQuery, "spec.nodeName") {
				glog.V(8).Info("Add nodeName selector to query: %s", req.URL)
				if len(req.URL.RawQuery) == 0 {
					req.URL.RawQuery = fmt.Sprintf("fieldSelector=spec.nodeName=%s", node)
				} else {
					req.URL.RawQuery += fmt.Sprintf("&fieldSelector=spec.nodeName=%s", node)
				}
			}
		}

		// Match Node requests
		matched, err = regexp.MatchString(`^/api/v1/nodes$`, req.URL.Path)
		if err != nil {
			return
		}
		if matched {
			if !strings.Contains(req.URL.RawQuery, "metadata.name") {
				glog.V(8).Info("Add nodeName selector to query: %s", req.URL)
				if len(req.URL.RawQuery) == 0 {
					req.URL.RawQuery = fmt.Sprintf("fieldSelector=metadata.name=%s", node)
				} else {
					req.URL.RawQuery += fmt.Sprintf("&fieldSelector=metadata.name=%s", node)
				}
			}
		}

		// Block outgoing requests if they request resources "services" or "endpoints"
		if strings.Contains(req.URL.Path, "services") || strings.Contains(req.URL.Path, "endpoints") {
			req.Header.Add(XRoutingBlockHeader, fmt.Sprintf("Request blocked in proxy.Director: %s", req.URL.Path))
		}
	}

	// Don't buffer the response for too long. Buffering indefinitely interferes
	// with watch notifications and causes pod creation and deletion to hang.
	// Note - a 100ms flush time is used rather than immediate flushing because
	// 				ReverseProxy in this version of go (1.11.1) doesn't support
	//				immediate flushing.
	proxy.FlushInterval = 100 * time.Millisecond
	proxy.ModifyResponse = func(resp *http.Response) error {
		if resp.StatusCode == http.StatusBadGateway {
			// fog-proxy is expected to return 502 if the request timed out.
			// Return an error in this case so it is treated the same an an actual timeout.
			return fmt.Errorf("Passing request to error handler")
		}
		return nil
	}
	proxy.ErrorHandler = func(resp http.ResponseWriter, req *http.Request, err error) {
		wsContainer.ServeHTTP(resp, req)
	}
	c.server.Handler = proxy

	return c, nil
}

func (c *CachingServer) get(r *restful.Request, w *restful.Response) {
	var options metav1.GetOptions
	err := metav1.ParameterCodec.DecodeParameters(r.Request.URL.Query(), metav1.SchemeGroupVersion, &options)
	if err != nil {
		glog.Warningf("Error decoding parameters: %v", err)
		w.WriteHeader(http.StatusBadGateway)
		return
	}

	getResult, err := c.cache.Get(r.PathParameter("resource"), r.PathParameter("namespace"), r.PathParameter("name"), options)
	if err != nil {
		glog.Warningf("Get error '%v'", err)
		w.WriteHeader(http.StatusBadGateway)
		return
	}

	data, err := json.Marshal(getResult)
	if err != nil {
		glog.Errorf("Marshalling failed")
		w.WriteHeader(http.StatusBadGateway)
		return
	}

	glog.Infof("Cache GET '%v'", r.Request.URL)
	w.Header().Add("Content-Type", "application/json")
	w.Write(data)
}

func (c *CachingServer) list(r *restful.Request, w *restful.Response) {
	var options metav1.ListOptions
	err := metav1.ParameterCodec.DecodeParameters(r.Request.URL.Query(), metav1.SchemeGroupVersion, &options)
	if err != nil {
		glog.Warningf("Error decoding parameters: %v", err)
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	if options.Watch {
		time.Sleep(KubeletWatchTimeout)
		glog.Infof("Unsupported Watch '%v'", r.Request.URL)
		w.WriteHeader(http.StatusBadGateway)
		return
	}

	listResult, err := c.cache.List(r.PathParameter("resource"), r.PathParameter("namespace"), options)
	if err != nil {
		glog.Warningf("List error '%v'", err)
		w.WriteHeader(http.StatusBadGateway)
		return
	}

	data, err := json.Marshal(listResult)
	if err != nil {
		glog.Errorf("Marshalling failed")
		w.WriteHeader(http.StatusBadGateway)
		return
	}

	glog.Infof("Cache LIST '%v'", r.Request.URL)
	w.Header().Add("Content-Type", "application/json")
	w.Write(data)
}

func (c *CachingServer) status(r *restful.Request, w *restful.Response) {
	glog.Infof("Unsupported Status %v '%v'", r.Request.Method, r.Request.URL)
	w.WriteHeader(http.StatusBadGateway)
}

func (c *CachingServer) LocalCache() LocalCache {
	return c.cache
}

func (c *CachingServer) Serve(listener net.Listener) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(2)
	go func() {
		c.cache.Run()
		wg.Done()
	}()
	go func() {
		<-c.ctx.Done()
		c.server.Close()
		wg.Done()
	}()

	err := c.server.Serve(listener)
	if err != nil {
		return err
	}

	return nil
}
