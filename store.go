package gaestore

import (
	"fmt"
	"reflect"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

// Entity is an interface for structs to be stored in the app engine
// datastore
type Entity interface {
	Key(ctx context.Context) *datastore.Key
}

type BeforePutter interface {
	BeforePut(ctx context.Context) error
}

type AfterPutter interface {
	AfterPut(ctx context.Context, key *datastore.Key) error
}

type AfterGetter interface {
	AfterGet(ctx context.Context, key *datastore.Key) error
}

type store struct {
	useCache bool
}

func (s *store) Put(ctx context.Context, e Entity) (*datastore.Key, error) {
	return put(ctx, e, s.useCache)
}

func (s *store) Get(ctx context.Context, e Entity) error {
	return get(ctx, e, s.useCache)
}

func (s *store) Query(ctx context.Context, q *datastore.Query, entities interface{}) (datastore.Cursor, error) {
	return query(ctx, q, s.useCache, entities)
}

func (s *store) Delete(ctx context.Context, e Entity) error {
	return delete(ctx, e)
}

func NewStore() *store {
	return &store{
		useCache: false,
	}
}

func NewStoreWithCache() *store {
	return &store{
		useCache: true,
	}
}

func Put(ctx context.Context, e Entity) (*datastore.Key, error) {
	return put(ctx, e, true)
}

func Get(ctx context.Context, e Entity) error {
	return get(ctx, e, true)
}

func Query(ctx context.Context, q *datastore.Query, entities interface{}) (datastore.Cursor, error) {
	return query(ctx, q, true, entities)
}

func Exists(ctx context.Context, e Entity) (bool, error) {
	err := get(ctx, e, false)
	switch err {
	case nil:
		return true, nil
	case datastore.ErrNoSuchEntity:
		return false, nil
	default:
		return false, err
	}
}

func PutCache(ctx context.Context, e Entity) error {
	k := e.Key(ctx)
	item := &memcache.Item{
		Key:    k.Encode(),
		Object: e,
	}
	return memcache.JSON.Set(ctx, item)
}

func GetCache(ctx context.Context, e Entity) (*memcache.Item, error) {
	key := e.Key(ctx)
	return getCache(ctx, key.Encode(), e)
}

func getCache(ctx context.Context, cacheKey string, dst Entity) (*memcache.Item, error) {
	return memcache.JSON.Get(ctx, cacheKey, dst)
}

func DeleteCache(ctx context.Context, e Entity) error {
	key := e.Key(ctx)
	return memcache.Delete(ctx, key.Encode())
}

func Delete(ctx context.Context, e Entity) error {
	return delete(ctx, e)
}

func beforePut(ctx context.Context, e Entity) error {
	if putter, ok := e.(BeforePutter); ok {
		return putter.BeforePut(ctx)
	}
	return nil
}

func afterGet(ctx context.Context, key *datastore.Key, e Entity) error {
	if getter, ok := e.(AfterGetter); ok {
		return getter.AfterGet(ctx, key)
	}
	return nil
}

func afterPut(ctx context.Context, key *datastore.Key, e Entity) error {
	if putter, ok := e.(AfterPutter); ok {
		return putter.AfterPut(ctx, key)
	}
	return nil
}

func put(ctx context.Context, e Entity, cache bool) (*datastore.Key, error) {
	if err := beforePut(ctx, e); err != nil {
		return nil, err
	}

	k, err := datastore.Put(ctx, e.Key(ctx), e)
	if err != nil {
		return nil, err
	}
	if err := afterPut(ctx, k, e); err != nil {
		return k, err
	}
	if cache {
		return k, PutCache(ctx, e)
	}
	return k, nil
}

func delete(ctx context.Context, e Entity) error {
	key := e.Key(ctx)
	err := datastore.Delete(ctx, key)
	if err != nil {
		return err
	}
	err = DeleteCache(ctx, e)
	if err != nil {
		fmt.Println(err)
	}
	return nil
}

func get(ctx context.Context, e Entity, useCache bool) error {
	k := e.Key(ctx)
	//if useCache {
	//_, err := GetCache(ctx, e)
	//switch err {
	//case nil:
	//return nil
	//case memcache.ErrCacheMiss:
	//err := getByKey(ctx, k, e, useCache)
	//if err != nil {
	//return err
	//}
	//// Since we had a cache miss, add it to cache
	//err = PutCache(ctx, e)
	//return nil
	//default:
	//return err
	//}
	//}
	return getByKey(ctx, k, e, useCache)
}

func getByKey(ctx context.Context, key *datastore.Key, e Entity, useCache bool) error {
	if useCache {
		_, err := getCache(ctx, key.Encode(), e)
		switch err {
		case nil:
			return nil
		case memcache.ErrCacheMiss:
			err := datastore.Get(ctx, key, e)
			if err != nil {
				return err
			}
			if err := afterGet(ctx, key, e); err != nil {
				return err
			}
			err = PutCache(ctx, e)
			if err != nil {
				fmt.Printf("Unable to put into cache [%v]", err)
			}
			return nil
		default:
			fmt.Printf("Error getting from cache [%v]\n", err)
		}
	}
	return datastore.Get(ctx, key, e)
}

func query(ctx context.Context, q *datastore.Query, useCache bool, entities interface{}) (c datastore.Cursor, err error) {
	var (
		dv       reflect.Value
		mat      multiArgType
		elemType reflect.Type
	)

	q = q.KeysOnly()
	t := q.Run(ctx)

	dv = reflect.ValueOf(entities)
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return c, fmt.Errorf("Invalid entity type for slice")
	}
	dv = dv.Elem()
	mat, elemType = checkMultiArg(dv)
	if mat == multiArgTypeInvalid || mat == multiArgTypeInterface {
		return c, fmt.Errorf("Invalid type")
	}
	for {
		key, err := t.Next(nil)
		if err == datastore.Done {
			break
		}
		if err != nil {
			fmt.Printf("Error fetching %v\n", err)
			break
		}
		ev := reflect.New(elemType)
		entity, ok := ev.Interface().(Entity)
		if !ok {
			fmt.Println("Not an Entity type")
			break
		}
		err = getByKey(ctx, key, entity, useCache)
		if err != nil {
			fmt.Println(err)
		}
		if mat != multiArgTypeStructPtr {
			ev = ev.Elem()
		}
		dv.Set(reflect.Append(dv, ev))
	}
	return t.Cursor()
}

type multiArgType int

const (
	multiArgTypeInvalid multiArgType = iota
	multiArgTypePropertyLoadSaver
	multiArgTypeStruct
	multiArgTypeStructPtr
	multiArgTypeInterface
)

// Took this from the official app engine source
func checkMultiArg(v reflect.Value) (m multiArgType, elemType reflect.Type) {
	if v.Kind() != reflect.Slice {
		return multiArgTypeInvalid, nil
	}
	elemType = v.Type().Elem()
	switch elemType.Kind() {
	case reflect.Struct:
		return multiArgTypeStruct, elemType
	case reflect.Interface:
		return multiArgTypeInterface, elemType
	case reflect.Ptr:
		elemType = elemType.Elem()
		if elemType.Kind() == reflect.Struct {
			return multiArgTypeStructPtr, elemType
		}
	}
	return multiArgTypeInvalid, nil
}
