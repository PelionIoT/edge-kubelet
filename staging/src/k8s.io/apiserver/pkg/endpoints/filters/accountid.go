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

/*
 * TODO argus
 */

package filters

import (
	"net/http"

	utilheaders "k8s.io/apiserver/pkg/util/headers"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// WithAccountID will extract an accountID from the header and set it on the context
func WithAccountID(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		accountid := utilheaders.ExtractAccountID(req, "generic")
		req = req.WithContext(genericapirequest.WithAccountID(req.Context(), accountid))
		handler.ServeHTTP(w, req)
	})
}
