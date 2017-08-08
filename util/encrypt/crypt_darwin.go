// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package encrypt

import (
	"sync"
	"unsafe"
)

/*
#include <stdlib.h>
#include <unistd.h>
*/
import "C"

var (
	mu sync.Mutex
)

// Crypt wrapper unix crypt() system call and return a string.
func Crypt(key, salt string) (string, error) {
	c_key := C.CString(key)
	defer C.free(unsafe.Pointer(c_key))

	c_salt := C.CString(salt)
	defer C.free(unsafe.Pointer(c_salt))

	// crypt() is not thread safe.
	var res string
	mu.Lock()
	c_res, err := C.crypt(c_key, c_salt)
	if c_res != nil {
		res = C.GoString(c_res)
	}
	mu.Unlock()
	return res, err
}
