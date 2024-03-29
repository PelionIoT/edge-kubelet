/*
Copyright 2018-2020, Arm Limited and affiliates.
Copyright 2017 The Kubernetes Authors.

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

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/util/dryrun"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utiltrace "k8s.io/apiserver/pkg/util/trace"
	utilheaders "k8s.io/apiserver/pkg/util/headers"

	"github.com/golang/glog"
)

func inBlacklist(kind string) bool {
	switch kind {
	// These are disabled because we do not support
	//  a scheduler, and these treat nodes as fungible
	//  instead of specific.
	case
		"HorizontalPodAutoscaler",
		"Deployment",
		"ReplicaSet",
		"StatefulSet":
		return true
	// These are disabled because we do not support
	//  using Argus as part of the active control plane
	//  and can make no guarantees about connectivity to nodes.
	case
		"Job",
		"CronJob":
		return true
	}
	return false
}

func createHandler(r rest.NamedCreater, scope RequestScope, admit admission.Interface, includeName bool) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		// For performance tracking purposes.
		trace := utiltrace.New("Create " + req.URL.Path)
		defer trace.LogIfLong(500 * time.Millisecond)

		if isDryRun(req.URL) && !utilfeature.DefaultFeatureGate.Enabled(features.DryRun) {
			scope.err(errors.NewBadRequest("the dryRun alpha feature is disabled"), w, req)
			return
		}

		// TODO: we either want to remove timeout or document it (if we document, move timeout out of this function and declare it in api_installer)
		timeout := parseTimeout(req.URL.Query().Get("timeout"))

		// We can't get accountid from the URL, so don't bother trying.
		var (
			accountid, namespace, name string
			err                        error
		)
		if includeName {
			_, namespace, name, err = scope.Namer.Name(req)
		} else {
			namespace, err = scope.Namer.Namespace(req)
		}
		if err != nil {
			scope.err(err, w, req)
			return
		}
		accountid = utilheaders.ExtractAccountID(req, "create")

		ctx := req.Context()
		ctx = request.WithNamespace(ctx, namespace)
		ctx = request.WithAccountID(ctx, accountid)

		gv := scope.Kind.GroupVersion()
		s, err := negotiation.NegotiateInputSerializer(req, false, scope.Serializer)
		if err != nil {
			scope.err(err, w, req)
			return
		}
		decoder := scope.Serializer.DecoderToVersion(s.Serializer, scope.HubGroupVersion)

		body, err := readBody(req)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		options := &metav1.CreateOptions{}
		values := req.URL.Query()
		if err := metainternalversion.ParameterCodec.DecodeParameters(values, scope.MetaGroupVersion, options); err != nil {
			err = errors.NewBadRequest(err.Error())
			scope.err(err, w, req)
			return
		}
		if errs := validation.ValidateCreateOptions(options); len(errs) > 0 {
			err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "CreateOptions"}, "", errs)
			scope.err(err, w, req)
			return
		}

		defaultGVK := scope.Kind
		original := r.New()
		trace.Step("About to convert to expected version")
		obj, gvk, err := decoder.Decode(body, &defaultGVK, original)
		if err != nil {
			err = transformDecodeError(scope.Typer, err, original, gvk, body)
			scope.err(err, w, req)
			return
		}
		if gvk.GroupVersion() != gv {
			err = errors.NewBadRequest(fmt.Sprintf("the API version in the data (%s) does not match the expected API version (%v)", gvk.GroupVersion().String(), gv.String()))
			scope.err(err, w, req)
			return
		}
		trace.Step("Conversion done")

		// Argus creation blacklist for unsupported types.
		if inBlacklist(gvk.Kind) {
			glog.Errorf("Denying object due to blacklist status: %s/%s/%s, %#v", accountid, namespace, name, gvk)
			scope.err(errors.NewMethodNotSupported(scope.Resource.GroupResource(), "post"), w, req)
			return
		}

		ae := request.AuditEventFrom(ctx)
		admit = admission.WithAudit(admit, ae)
		audit.LogRequestObject(ae, obj, scope.Resource, scope.Subresource, scope.Serializer)

		userInfo, _ := request.UserFrom(ctx)
		admissionAttributes := admission.NewAttributesRecord(obj, nil, scope.Kind, accountid, namespace, name, scope.Resource, scope.Subresource, admission.Create, dryrun.IsDryRun(options.DryRun), userInfo)
		if mutatingAdmission, ok := admit.(admission.MutationInterface); ok && mutatingAdmission.Handles(admission.Create) {
			err = mutatingAdmission.Admit(admissionAttributes)
			if err != nil {
				scope.err(err, w, req)
				return
			}
		}

		trace.Step("About to store object in database")
		result, err := finishRequest(timeout, func() (runtime.Object, error) {
			return r.Create(
				ctx,
				name,
				obj,
				rest.AdmissionToValidateObjectFunc(admit, admissionAttributes),
				options,
			)
		})
		if err != nil {
			scope.err(err, w, req)
			return
		}
		trace.Step("Object stored in database")

		requestInfo, ok := request.RequestInfoFrom(ctx)
		if !ok {
			scope.err(fmt.Errorf("missing requestInfo"), w, req)
			return
		}
		if err := setSelfLink(result, requestInfo, scope.Namer); err != nil {
			scope.err(err, w, req)
			return
		}
		trace.Step("Self-link added")

		// If the object is partially initialized, always indicate it via StatusAccepted
		code := http.StatusCreated
		if accessor, err := meta.Accessor(result); err == nil {
			if accessor.GetInitializers() != nil {
				code = http.StatusAccepted
			}
		}
		status, ok := result.(*metav1.Status)
		if ok && err == nil && status.Code == 0 {
			status.Code = int32(code)
		}

		scope.Trace = trace
		transformResponseObject(ctx, scope, req, w, code, result)
	}
}

// CreateNamedResource returns a function that will handle a resource creation with name.
func CreateNamedResource(r rest.NamedCreater, scope RequestScope, admission admission.Interface) http.HandlerFunc {
	return createHandler(r, scope, admission, true)
}

// CreateResource returns a function that will handle a resource creation.
func CreateResource(r rest.Creater, scope RequestScope, admission admission.Interface) http.HandlerFunc {
	return createHandler(&namedCreaterAdapter{r}, scope, admission, false)
}

type namedCreaterAdapter struct {
	rest.Creater
}

func (c *namedCreaterAdapter) Create(ctx context.Context, name string, obj runtime.Object, createValidatingAdmission rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	return c.Creater.Create(ctx, obj, createValidatingAdmission, options)
}
