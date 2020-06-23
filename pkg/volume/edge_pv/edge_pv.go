/*
Copyright 2018-2020, Arm Limited and affiliates.
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package edge_pv

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"strings"

	fog_net "github.com/armPelionEdge/fog-proxy/net"
	"github.com/emicklei/go-restful"
	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/recyclerclient"
	"k8s.io/kubernetes/pkg/volume/validation"
)

type EdgePV interface {
	Init(volume.VolumeHost)
	Provision(gatewayId string, pvName string) error
	Delete(gatewayId string, pvName string) error
	Recycle(gatewayId string, pvName string) error
	GetVolumeSource(spec *volume.Spec) (*v1.HostPathVolumeSource, bool, error)
}

func EdgePVHandler(path string, provisioner volume.ProvisionableVolumePlugin, deleter volume.DeletableVolumePlugin, recycler volume.RecyclableVolumePlugin) *restful.WebService {
	ws := new(restful.WebService)

	server := &edgePVService{
		provisioner: provisioner,
		deleter:     deleter,
		recycler:    recycler,
	}

	ws.Path(path)
	ws.Route(ws.PUT("/{pvName}").
		To(server.provision).
		Operation("provisionEdgePV"))
	ws.Route(ws.DELETE("/{pvName}").
		To(server.delete).
		Operation("deleteEdgePV"))
	ws.Route(ws.DELETE("/{pvName}/contents").
		To(server.recycle).
		Operation("recycleEdgePV"))

	return ws
}

type edgePVService struct {
	provisioner volume.ProvisionableVolumePlugin
	deleter     volume.DeletableVolumePlugin
	recycler    volume.RecyclableVolumePlugin
}

func (service *edgePVService) provision(request *restful.Request, response *restful.Response) {
	pvName := request.PathParameter("pvName")

	glog.Infof("Received edge-pv provision request: %s", pvName)

	provisioner, err := service.provisioner.NewProvisioner(volume.VolumeOptions{PVName: pvName, PVC: &v1.PersistentVolumeClaim{}})

	if err != nil {
		glog.Errorf("Could not create provisioner for %s: %s", pvName, err.Error())
		response.WriteError(http.StatusInternalServerError, fmt.Errorf("Could not create provisioner for %s: %s", pvName, err.Error()))

		return
	}

	_, err = provisioner.Provision(nil, nil)

	if err != nil {
		glog.Errorf("Could not provision %s: %s", pvName, err.Error())
		response.WriteError(http.StatusInternalServerError, fmt.Errorf("Could not provision %s: %s", pvName, err.Error()))

		return
	}

	glog.Infof("Provisioned edge-pv: %s", pvName)

	response.WriteHeader(http.StatusOK)
}

func (service *edgePVService) delete(request *restful.Request, response *restful.Response) {
	pvName := request.PathParameter("pvName")

	glog.Infof("Received edge-pv delete request: %s", pvName)

	spec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName}}}
	spec.PersistentVolume.ObjectMeta.Annotations = map[string]string{AnnGateway: "abc"}
	spec.PersistentVolume.Spec.HostPath = &v1.HostPathVolumeSource{Path: "abc"}

	deleter, err := service.deleter.NewDeleter(spec)

	if err != nil {
		glog.Errorf("Could not create deleter for %s: %s", pvName, err.Error())

		response.WriteError(http.StatusInternalServerError, fmt.Errorf("Could not create deleter for %s: %s", pvName, err.Error()))

		return
	}

	if err := deleter.Delete(); err != nil {
		glog.Errorf("Could not delete %s: %s", pvName, err.Error())

		response.WriteError(http.StatusInternalServerError, fmt.Errorf("Could not delete %s: %s", pvName, err.Error()))

		return
	}

	glog.Infof("Deleted edge-pv: %s", pvName)

	response.WriteHeader(http.StatusOK)
}

func (service *edgePVService) recycle(request *restful.Request, response *restful.Response) {
	pvName := request.PathParameter("pvName")

	glog.Infof("Received edge-pv recycle request: %s", pvName)

	spec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName}}}
	spec.PersistentVolume.Spec.HostPath = &v1.HostPathVolumeSource{}

	if err := service.recycler.Recycle(pvName, spec, nil); err != nil {
		glog.Errorf("Could not recycle %s: %s", pvName, err.Error())

		response.WriteError(http.StatusInternalServerError, fmt.Errorf("Could not recycle %s: %s", pvName, err.Error()))

		return
	}

	glog.Infof("Recycled edge-pv: %s", pvName)

	response.WriteHeader(http.StatusOK)
}

// Uses fog-proxy
type EdgePVClient struct {
	client *http.Client
}

func (edgePVClient *EdgePVClient) Init(host volume.VolumeHost) {
}

func (edgePVClient *EdgePVClient) doRequest(gateway, method, path string) error {
	req, err := http.NewRequest(method, fmt.Sprintf("http://%s%s", gateway, path), nil)

	if err != nil {
		return fmt.Errorf("Could not create new request: %s", err.Error())
	}

	resp, err := edgePVClient.client.Do(req)

	if err != nil {
		return fmt.Errorf("Could not do requst: %s", err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Received status code %d", resp.StatusCode)
	}

	return nil
}

func (edgePVClient *EdgePVClient) Provision(gatewayId string, pvName string) error {
	return edgePVClient.doRequest(gatewayId, "PUT", fmt.Sprintf("/edge-pv/%s", pvName))
}

func (edgePVClient *EdgePVClient) Delete(gatewayId string, pvName string) error {
	return edgePVClient.doRequest(gatewayId, "DELETE", fmt.Sprintf("/edge-pv/%s", pvName))
}

func (edgePVClient *EdgePVClient) Recycle(gatewayId string, pvName string) error {
	return edgePVClient.doRequest(gatewayId, "DELETE", fmt.Sprintf("/edge-pv/%s/contents", pvName))
}

func (edgePVClient *EdgePVClient) GetVolumeSource(spec *volume.Spec) (*v1.HostPathVolumeSource, bool, error) {
	return nil, false, nil
}

type EdgePVManager struct {
	EdgePVDirectory string
	host            volume.VolumeHost
}

func (edgePVManager *EdgePVManager) Init(host volume.VolumeHost) {
	edgePVManager.EdgePVDirectory = host.GetPluginDir(EdgePVPluginName)
	edgePVManager.host = host
}

// Creates directory
func (edgePVManager *EdgePVManager) Provision(gatewayId string, pvName string) error {
	hostPath := edgePVManager.pvHostPath(pvName)

	if err := os.MkdirAll(hostPath, 0750); err != nil {
		return err
	}

	return nil
}

// Delete directory and free up resources
func (edgePVManager *EdgePVManager) Delete(gatewayId string, pvName string) error {
	hostPath := edgePVManager.pvHostPath(pvName)

	if err := os.RemoveAll(hostPath); err != nil {
		return err
	}

	return nil
}

// Recycle should delete contents of the directory to return it to a blank state
func (edgePVManager *EdgePVManager) Recycle(gatewayId string, pvName string) error {
	hostPath := edgePVManager.pvHostPath(pvName)
	dir, err := ioutil.ReadDir(hostPath)

	if err != nil {
		return err
	}

	for _, d := range dir {
		if err := os.RemoveAll(path.Join(edgePVManager.EdgePVDirectory, d.Name())); err != nil {
			return err
		}
	}

	return nil
}

// Get the directory host path for this volume
func (edgePVManager *EdgePVManager) GetVolumeSource(spec *volume.Spec) (*v1.HostPathVolumeSource, bool, error) {
	if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.HostPath != nil && metav1.HasAnnotation(spec.PersistentVolume.ObjectMeta, AnnGateway) {

		if edgePVManager.host.GetHostName() != spec.PersistentVolume.GetAnnotations()[AnnGateway] {
			return nil, false, fmt.Errorf("This volume is not provisioned for this gateway")
		}

		return &v1.HostPathVolumeSource{
			Path: edgePVManager.pvHostPath(spec.PersistentVolume.Name),
			Type: spec.PersistentVolume.Spec.HostPath.Type,
		}, spec.ReadOnly, nil
	}

	return nil, false, fmt.Errorf("Spec does not reference an Edge PV volume type")
}

func (edgePVManager *EdgePVManager) pvHostPath(pvName string) string {
	return path.Join(edgePVManager.EdgePVDirectory, pvName)
}

// This is the primary entrypoint for volume plugins.
// The volumeConfig arg provides the ability to configure volume behavior.  It is implemented as a pointer to allow nils.
// The hostPathPlugin is used to store the volumeConfig and give it, when needed, to the func that Recycles.
// Tests that exercise recycling should not use this func but instead use ProbeRecyclablePlugins() to override default behavior.
func ProbeVolumePlugins(volumeConfig volume.VolumeConfig) []volume.VolumePlugin {
	return []volume.VolumePlugin{
		&edgePVPlugin{
			host:   nil,
			config: volumeConfig,
		},
	}
}

func Fake(edgePV EdgePV) volume.VolumePlugin {
	return &edgePVPlugin{edgePV: edgePV}
}

type edgePVPlugin struct {
	host   volume.VolumeHost
	config volume.VolumeConfig
	edgePV EdgePV
}

var _ volume.VolumePlugin = &edgePVPlugin{}
var _ volume.PersistentVolumePlugin = &edgePVPlugin{}
var _ volume.RecyclableVolumePlugin = &edgePVPlugin{}
var _ volume.DeletableVolumePlugin = &edgePVPlugin{}
var _ volume.ProvisionableVolumePlugin = &edgePVPlugin{}

const (
	EdgePVPluginName         = "kubernetes.io/edge-pv"
	AnnGateway               = "volume.kubernetes.io/gateway"
	EdgePVFogProxyAddress    = "edge-pv-fog-proxy-address"
	EdgePVProvisionerAddress = "edge-pv-provisioner-address"
)

func (plugin *edgePVPlugin) Init(host volume.VolumeHost) error {
	// Could we use the VolumeHost interface to inject a handle to edge-pv-provisioner?
	plugin.host = host

	if host.GetPluginDir(EdgePVPluginName) == "" {
		// Assume this is kube-controller-manager
		fogProxyAddress := plugin.config.OtherAttributes[EdgePVFogProxyAddress]
		edgePVProvisionerAddress := plugin.config.OtherAttributes[EdgePVProvisionerAddress]

		plugin.edgePV = &EdgePVClient{
			client: &http.Client{
				Transport: &http.Transport{
					DialContext: func(ctx context.Context, net, addr string) (net.Conn, error) {
						parts := strings.Split(addr, ":")

						if len(parts) == 0 {
							return nil, fmt.Errorf("Unable to parse fog proxy address %s", addr)
						}

						glog.Infof("fog_net.DialNodeTCP(%s, %s, %s)", fogProxyAddress, parts[0], edgePVProvisionerAddress)

						return fog_net.DialNodeTCP(fogProxyAddress, parts[0], edgePVProvisionerAddress)
					},
				},
			},
		}
	} else {
		// Assume this is kubelet
		plugin.edgePV = &EdgePVManager{}
		plugin.edgePV.Init(host)
	}

	return nil
}

func (plugin *edgePVPlugin) GetPluginName() string {
	return EdgePVPluginName
}

func (plugin *edgePVPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _, err := plugin.edgePV.GetVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return volumeSource.Path, nil
}

func (plugin *edgePVPlugin) CanSupport(spec *volume.Spec) bool {
	return spec.PersistentVolume != nil && spec.PersistentVolume.Spec.HostPath != nil && metav1.HasAnnotation(spec.PersistentVolume.ObjectMeta, AnnGateway)
}

func (plugin *edgePVPlugin) RequiresRemount() bool {
	return false
}

func (plugin *edgePVPlugin) SupportsMountOption() bool {
	return false
}

func (plugin *edgePVPlugin) SupportsBulkVolumeVerification() bool {
	return false
}

func (plugin *edgePVPlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
	}
}

func (plugin *edgePVPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod, opts volume.VolumeOptions) (volume.Mounter, error) {
	hostPathVolumeSource, readOnly, err := plugin.edgePV.GetVolumeSource(spec)
	if err != nil {
		return nil, err
	}

	path := hostPathVolumeSource.Path
	pathType := new(v1.HostPathType)
	if hostPathVolumeSource.Type == nil {
		*pathType = v1.HostPathUnset
	} else {
		pathType = hostPathVolumeSource.Type
	}

	return &edgePVMounter{
		edgePV:   &edgePV{path: path, pathType: pathType},
		readOnly: readOnly,
		mounter:  plugin.host.GetMounter(plugin.GetPluginName()),
	}, nil
}

func (plugin *edgePVPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return &edgePVUnmounter{&edgePV{
		path: "",
	}}, nil
}

func (plugin *edgePVPlugin) Recycle(pvName string, spec *volume.Spec, eventRecorder recyclerclient.RecycleEventRecorder) error {
	if spec.PersistentVolume == nil || spec.PersistentVolume.Spec.HostPath == nil {
		return fmt.Errorf("spec.PersistentVolume.Spec.HostPath is nil")
	}
	gatewayId := spec.PersistentVolume.GetObjectMeta().GetAnnotations()[AnnGateway]

	return plugin.edgePV.Recycle(gatewayId, pvName)
}

func (plugin *edgePVPlugin) NewDeleter(spec *volume.Spec) (volume.Deleter, error) {
	return newDeleter(spec, plugin.host, plugin)
}

func (plugin *edgePVPlugin) NewProvisioner(options volume.VolumeOptions) (volume.Provisioner, error) {
	return newProvisioner(options, plugin.host, plugin)
}

func (plugin *edgePVPlugin) ConstructVolumeSpec(volumeName, mountPath string) (*volume.Spec, error) {
	hostPathVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: "",
			},
		},
	}
	return volume.NewSpecFromVolume(hostPathVolume), nil
}

func newDeleter(spec *volume.Spec, host volume.VolumeHost, plugin *edgePVPlugin) (volume.Deleter, error) {
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.HostPath == nil {
		return nil, fmt.Errorf("spec.PersistentVolumeSource.HostPath is nil")
	}

	return &edgePVDeleter{edgePV: plugin.edgePV, name: spec.Name(), gatewayId: spec.PersistentVolume.GetObjectMeta().GetAnnotations()[AnnGateway]}, nil
}

func newProvisioner(options volume.VolumeOptions, host volume.VolumeHost, plugin *edgePVPlugin) (volume.Provisioner, error) {
	return &edgePVProvisioner{options: options, plugin: plugin, edgePV: plugin.edgePV}, nil
}

// HostPath volumes represent a bare host file or directory mount.
// The direct at the specified path will be directly exposed to the container.
type edgePV struct {
	path     string
	pathType *v1.HostPathType
	volume.MetricsNil
}

func (hp *edgePV) GetPath() string {
	return hp.path
}

type edgePVMounter struct {
	*edgePV
	readOnly bool
	mounter  mount.Interface
}

var _ volume.Mounter = &edgePVMounter{}

func (b *edgePVMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:        b.readOnly,
		Managed:         false,
		SupportsSELinux: false,
	}
}

// Checks prior to mount operations to verify that the required components (binaries, etc.)
// to mount the volume are available on the underlying node.
// If not, it returns an error
func (b *edgePVMounter) CanMount() error {
	return nil
}

// SetUp does nothing.
func (b *edgePVMounter) SetUp(fsGroup *int64) error {
	err := validation.ValidatePathNoBacksteps(b.GetPath())

	if err != nil {
		return fmt.Errorf("invalid HostPath `%s`: %v", b.GetPath(), err)
	}

	if err := checkType(b.GetPath(), b.pathType, b.mounter); err != nil {
		return fmt.Errorf("Check type error: %v", err)
	}

	if err := volume.SetVolumeOwnership(b, fsGroup); err != nil {
		return fmt.Errorf("Could not set volume ownership: %v", err)
	}

	return nil
}

// SetUpAt does not make sense for host paths - probably programmer error.
func (b *edgePVMounter) SetUpAt(dir string, fsGroup *int64) error {
	return fmt.Errorf("SetUpAt() does not make sense for edge pv volumes")
}

func (b *edgePVMounter) GetPath() string {
	return b.path
}

type edgePVUnmounter struct {
	*edgePV
}

var _ volume.Unmounter = &edgePVUnmounter{}

// TearDown does nothing.
func (c *edgePVUnmounter) TearDown() error {
	return nil
}

// TearDownAt does not make sense for host paths - probably programmer error.
func (c *edgePVUnmounter) TearDownAt(dir string) error {
	return fmt.Errorf("TearDownAt() does not make sense for host paths")
}

// edgePVProvisioner implements a Provisioner for the EdgePV plugin
// This implementation is meant for testing only and only works in a single node cluster.
type edgePVProvisioner struct {
	options volume.VolumeOptions
	plugin  *edgePVPlugin
	edgePV  EdgePV
}

func (r *edgePVProvisioner) Provision(selectedNode *v1.Node, allowedTopologies []v1.TopologySelectorTerm) (*v1.PersistentVolume, error) {
	if util.CheckPersistentVolumeClaimModeBlock(r.options.PVC) {
		return nil, fmt.Errorf("%s does not support block volume provisioning", r.plugin.GetPluginName())
	}

	if r.options.PVName == "" {
		return nil, fmt.Errorf("No PVName specified")
	}

	gatewayId := r.options.PVC.GetObjectMeta().GetAnnotations()[AnnGateway]
	capacity := r.options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	t := v1.HostPathDirectory

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: r.options.PVName,
			Annotations: map[string]string{
				util.VolumeDynamicallyCreatedByKey: "edgepv-dynamic-provisioner",
				AnnGateway:                         gatewayId,
			},
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: r.options.PersistentVolumeReclaimPolicy,
			AccessModes:                   r.options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): capacity,
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: r.options.PVName,
					Type: &t,
				},
			},
		},
	}

	if len(r.options.PVC.Spec.AccessModes) == 0 {
		pv.Spec.AccessModes = r.plugin.GetAccessModes()
	}

	return pv, r.edgePV.Provision(gatewayId, r.options.PVName)
}

type edgePVDeleter struct {
	volume.MetricsNil
	name      string
	gatewayId string
	edgePV    EdgePV
}

func (r *edgePVDeleter) GetPath() string {
	return r.gatewayId
}

func (r *edgePVDeleter) Delete() error {
	return r.edgePV.Delete(r.gatewayId, r.name)
}

type edgePVTypeChecker interface {
	Exists() bool
	IsDir() bool
	GetPath() string
}

type fileTypeChecker struct {
	path    string
	exists  bool
	mounter mount.Interface
}

func (ftc *fileTypeChecker) Exists() bool {
	exists, err := ftc.mounter.ExistsPath(ftc.path)

	return exists && err == nil
}

func (ftc *fileTypeChecker) IsDir() bool {
	if !ftc.Exists() {
		return false
	}
	pathType, err := ftc.mounter.GetFileType(ftc.path)
	if err != nil {
		return false
	}
	return string(pathType) == string(v1.HostPathDirectory)
}

func (ftc *fileTypeChecker) GetPath() string {
	return ftc.path
}

func newFileTypeChecker(path string, mounter mount.Interface) edgePVTypeChecker {
	return &fileTypeChecker{path: path, mounter: mounter}
}

// checkType checks whether the given path is the exact pathType
func checkType(path string, pathType *v1.HostPathType, mounter mount.Interface) error {
	return checkTypeInternal(newFileTypeChecker(path, mounter), pathType)
}

func checkTypeInternal(ftc edgePVTypeChecker, pathType *v1.HostPathType) error {
	if *pathType != v1.HostPathDirectory {
		return fmt.Errorf("edgePV type check failed: pathType must be %s", v1.HostPathDirectory)
	}

	if !ftc.Exists() {
		return fmt.Errorf("edgePV type check failed: %s does not exist", ftc.GetPath())
	}

	if !ftc.IsDir() {
		return fmt.Errorf("edgePV type check failed: %s is not a directory", ftc.GetPath())
	}

	return nil
}
