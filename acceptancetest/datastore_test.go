// +build acceptance

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

package acceptancetest

import (
	"github.com/ExpediaGroup/flyte/httputil"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
)

const datastoreItem = `
{
  "124" : "XYZ",
  "345" : "ABC"
}
`

var DatastoreFeatures = []Test{
	{"Store New Item", StoreNewItem},
	{"Store Existing Item", StoreAlreadyExistingItem},
	{"Get Item", GetItem},
	{"Delete Item", DeleteItem},
	{"Get Non Existant Item", GetNonExistantItem},
	{"Delete Non Existant Item", DeleteNonExistantItem},
}

func StoreNewItem(t *testing.T) {
	form := map[string]string{"description": "some rubbish"}
	loc := flyteApi.DatastoreURL() + "/techops"
	resp, err := httpClient.PutMultipart(loc, form, []byte(datastoreItem), httputil.MediaTypeJson)
	if err != nil {
		t.Fatalf("Error registering datastore item: %s", err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Error registering datastore item \n Expecting status code 201, got %d", resp.StatusCode)
	}

	u, err := url.Parse(loc)
	require.NoError(t, err)
	httpClient.DeleteResource(t, u)
}

func GetItem(t *testing.T) {
	loc := addDatastoreItem(t)
	defer httpClient.DeleteResource(t, loc)

	resp, err := httpClient.Get(loc.String())
	if err != nil {
		t.Fatalf("Error getting datastore item: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %v: Expecting status code 200, got %d", loc, resp.StatusCode)
	}

	item, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Error reading datastore item body: %s", err)
	}

	if string(item) != datastoreItem {
		t.Fatalf("Expected item '%s', but got %s", datastoreItem, string(item))
	}
}

func DeleteItem(t *testing.T) {
	loc := addDatastoreItem(t)

	resp, err := httpClient.Delete(loc.String())
	if err != nil {
		t.Fatalf("Could not delete datastore item: %s", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("Expected status code '204', but got '%v'", resp.StatusCode)
	}
}

func StoreAlreadyExistingItem(t *testing.T) {
	loc := addDatastoreItem(t)
	defer httpClient.DeleteResource(t, loc)

	// add the same item
	form := map[string]string{"description": "new rubbish"}
	resp, err := httpClient.PutMultipart(flyteApi.DatastoreURL()+"/techops", form, []byte(datastoreItem), httputil.MediaTypeJson)
	if err != nil {
		t.Fatalf("Error registering datastore item: %s", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("Add existing datastore \n Expecting status code 204, got %d", resp.StatusCode)
	}
}

func GetNonExistantItem(t *testing.T) {
	loc := addDatastoreItem(t)
	defer httpClient.DeleteResource(t, loc)

	resp, err := httpClient.Get(loc.String() + "some-nonsense")
	if err != nil {
		t.Fatalf("Error getting non existing item %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Error getting non existing item \n Expecting status code 404, got %d", resp.StatusCode)
	}
}

func DeleteNonExistantItem(t *testing.T) {
	loc := addDatastoreItem(t)
	defer httpClient.DeleteResource(t, loc)

	resp, err := httpClient.Delete(loc.String() + "some-nonsense")
	if err != nil {
		t.Fatalf("Error deleting non existing item %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Error deleting non existing item \n Expecting status code 404, got %d", resp.StatusCode)
	}
}

func addDatastoreItem(t *testing.T) *url.URL {
	form := map[string]string{"description": "some rubbish"}
	loc := flyteApi.DatastoreURL() + "/techops"
	_, err := httpClient.PutMultipart(loc, form, []byte(datastoreItem), httputil.MediaTypeJson)
	if err != nil {
		t.Fatalf("Error registering datastore item: %s", err)
	}
	u, err := url.Parse(loc)
	require.NoError(t, err)
	return u
}
