/*
Copyright The Volcano Authors.

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

package sessionsticky

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
)

// MappingKey returns an opaque store key for a ModelServer and raw session material.
func MappingKey(modelServer types.NamespacedName, sessionMaterial string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%s|%s", modelServer.Namespace, modelServer.Name, sessionMaterial)))
	return "kthena/sticky/" + hex.EncodeToString(sum[:])
}
