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

package headers

import (
	"net/http"
	"runtime"
	"github.com/golang/glog"
)

const (
	AccountIdHeader string = "X-Auth-Account-Id"
	NodeNameHeader  string = "X-Auth-Node-Name"
)

func ExtractAccountID(req *http.Request, action string) string {
	accountid := ""
	accountids, ok := req.Header[AccountIdHeader]
	if ok {
		if len(accountids) > 0 {
			accountid = accountids[0]
		}
	}
	return accountid
}

func ExtractNodename(req *http.Request) string {
	nodeName := ""
	nns, ok := req.Header[NodeNameHeader]
	if ok {
		if len(nns) > 0 {
			nodeName = nns[0]
		}
	}
	return nodeName
}


func GlogStack() {
	stack := make([]byte, 500*1024)
	stack = stack[:runtime.Stack(stack, true)]
	glog.Errorf("!!!  Stack at this time: %s", stack)
	return
}
