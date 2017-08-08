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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testEncryptSuite) TestEncrypt(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		key     string
		salt    string
		expect  string
		isError bool
	}{
		{"pingcap", "1234567890123456", "123vIptKvplIA", false},
		{"hello", "aa", "aaPwJ9XL9Y99E", false},
		{"foo", "ff", "ffTU0fyIP09Z.", false},
	}

	for _, t := range tests {
		crypted, err := Crypt(t.key, t.salt)
		if t.isError {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(crypted, Equals, t.expect)
	}
}
