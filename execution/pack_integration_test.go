// +build integration

/*
Copyright (C) 2018 Expedia Group.

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

package execution

import (
	"fmt"
	"github.com/HotelsDotCom/flyte/mongo"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestTakeAction_ShouldBeAtomic(t *testing.T) {

	mongoT.DropDatabase(t)

	for i := 1; i < 20; i++ {
		mongoT.Insert(t, mongo.ActionCollectionId, newPackActionT("packA", strconv.Itoa(i), "actionA", stateNew, time.Now()))
	}

	goroutines := 20
	out := make(chan string, goroutines)

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			got, _ := Pack{Id: "packA", Name: "packA"}.TakeAction("actionA")
			out <- got.Id
		}(g)
	}

	var ids[] string
	for g := 0; g < goroutines; g++ {
		ids = append(ids, <-out)
	}

	assert.Equal(t, len(ids), len(unique(ids)), fmt.Sprintf("got %v", ids))
}

func unique(stringSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range stringSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}
