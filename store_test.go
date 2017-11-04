package gaestore

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

type object struct {
	ID   string
	Name string
}

func (o object) Key(ctx context.Context) *datastore.Key {
	return datastore.NewKey(ctx, "object", o.ID, 0, nil)
}
func TestQuery(t *testing.T) {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}

	defer done()
	tests := []struct {
		Entities []*object
	}{
		{
			[]*object{
				&object{
					ID:   "1",
					Name: "John",
				},
				&object{
					ID:   "2",
					Name: "Winston",
				},
				&object{
					ID:   "3",
					Name: "Finley",
				},
			},
		},
	}

	for _, test := range tests {
		for _, o := range test.Entities {
			_, err := Put(ctx, o)
			if err != nil {
				t.Fatal(err)
			}
		}
		// Hack to deal with eventual consistency
		time.Sleep(2 * time.Second)
		q := datastore.NewQuery("object")
		var entities []object
		_, err := Query(ctx, q, &entities)
		if err != nil {
			t.Fatal(err)
		}
		expected := len(test.Entities)
		length := len(entities)
		if length != expected {
			t.Fatalf("Expected to find [%v]  entities but got [%v]", expected, length)
		}
	}

}

func TestCrud(t *testing.T) {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	tests := []struct {
		Entity *object
	}{
		{
			Entity: &object{
				ID:   "1",
				Name: "John",
			},
		},
	}

	for _, test := range tests {
		s := NewStore()

		// Test Put
		k, err := s.Put(ctx, test.Entity)
		if err != nil {
			t.Fatal(err)
		}
		if k == nil {
			t.Fatal("Key is nil")
		}

		var o object
		err = datastore.Get(ctx, test.Entity.Key(ctx), &o)
		if err != nil {
			t.Fatal(err)
		}
		err = compare(test.Entity, &o)
		if err != nil {
			t.Fatal(err)
		}

		// Test Get
		o = object{
			ID: test.Entity.ID,
		}

		err = s.Get(ctx, &o)
		if err != nil {
			t.Fatal(err)
		}
		err = compare(test.Entity, &o)
		if err != nil {
			t.Fatal(err)
		}

		// Test Delete
		o = object{
			ID: test.Entity.ID,
		}
		err = s.Delete(ctx, o)
		if err != nil {
			t.Fatal("Unable to delete entity from datastore [%v]", err)
		}

		o = object{}
		err = datastore.Get(ctx, test.Entity.Key(ctx), &o)
		if err != datastore.ErrNoSuchEntity {
			t.Fatal("Expected entity to be deleted")
		}

		// Test Put with Cache
		_, err = Put(ctx, test.Entity)
		if err != nil {
			t.Fatal(err)
		}

		var cacheObj object
		_, err = memcache.JSON.Get(ctx, test.Entity.Key(ctx).Encode(), &cacheObj)
		if err != nil {
			t.Fatal(err)
		}
		err = compare(test.Entity, &cacheObj)
		if err != nil {
			t.Fatal(err)
		}

		o = object{}
		err = datastore.Get(ctx, test.Entity.Key(ctx), &o)
		if err != nil {
			t.Fatal(err)
		}
		err = compare(test.Entity, &o)
		if err != nil {
			t.Fatal(err)
		}

		// Test Get with cache
		err = datastore.Delete(ctx, test.Entity.Key(ctx))
		if err != nil {
			t.Fatal(err)
		}

		// Now that item is deleted from datastore, retrieve from cache
		o = object{
			ID: test.Entity.ID,
		}
		err = Get(ctx, &o)
		if err != nil {
			t.Fatal(err)
		}

		err = compare(test.Entity, &o)
		if err != nil {
			t.Fatal(err)
		}

		// Test Get with cache miss and ensure item is re-added to cache
		_, err = Put(ctx, test.Entity)
		if err != nil {
			t.Fatal(err)
		}

		err = memcache.Delete(ctx, test.Entity.Key(ctx).Encode())
		if err != nil {
			t.Fatalf("Unable to delete directly from memcache [%v]", err)
		}

		o = object{
			ID: test.Entity.ID,
		}
		err = Get(ctx, &o)
		if err != nil {
			t.Fatalf("Unable to get entity [%v]", err)
		}
		o = object{}
		_, err = memcache.JSON.Get(ctx, test.Entity.Key(ctx).Encode(), &o)
		if err != nil {
			t.Fatalf("Unable to get directly from memcache [%v]", err)
		}
		err = compare(test.Entity, &o)
		if err != nil {
			t.Fatal(err)
		}

		o = object{
			ID: test.Entity.ID,
		}

		// Test Delete
		err = Delete(ctx, &o)
		if err != nil {
			t.Fatal(err)
		}

		err = datastore.Get(ctx, test.Entity.Key(ctx), &o)
		if err != datastore.ErrNoSuchEntity {
			t.Fatal("Expected entity to be deleted")
		}

		o = object{}
		_, err = memcache.JSON.Get(ctx, test.Entity.Key(ctx).Encode(), &o)
		if err != memcache.ErrCacheMiss {
			t.Fatalf("Expected cache miss")
		}
	}
}

func compare(o1, o2 *object) error {
	if o1.ID != o2.ID {
		return fmt.Errorf("Expected o1.ID to be [%s] but got [%s]", o1.ID, o2.ID)
	}
	if o1.Name != o2.Name {
		return fmt.Errorf("Expected o1.Name to be [%s] but got [%s]", o1.Name, o2.Name)
	}
	return nil
}
