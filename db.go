package db

import (
	"encoding/json"
	"time"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type KeyVal struct {
	Key          uint64      `json:"key,omitempty" bson:"_id,omitempty"`
	Val          interface{} `json:"val,omitempty" bson:"val,omitempty"`
	LastModified time.Time   `json:"lm,omitempty" bson:"lm,omitempty"`
}

type EntityPimp interface {
	Fetch(id uint64) (json.RawMessage, error)
	Parse(raw json.RawMessage) (interface{}, error)
}

type DB struct {
	*mgo.Collection
	EntityPimp
}

func NewDB(c *mgo.Collection, e EntityPimp) *DB {
	return &DB{c, e}
}

func (c *DB) GetKeyVal(kv *KeyVal) error {
	q := c.FindId(kv.Key)
	err := q.One(kv)
	if err != nil && err != mgo.ErrNotFound {
		return err
	}
	return nil
}

func (c *DB) PutKeyVal(kv *KeyVal) error {
	_, err := c.UpsertId(kv.Key, bson.M{"$set": kv})
	return err
}

func (c *DB) Refresh(id uint64) error {
	raw, err := c.Fetch(id)
	if err != nil {
		return nil
	}
	e, err := c.Parse(raw)
	if err != nil {
		return err
	}
	kv := &KeyVal{Key: id, Val: e, LastModified: time.Now()}
	c.PutKeyVal(kv)
	return nil
}

func (c *DB) RefreshShard(shardNum, numShards int) error {
	q := bson.M{"_id": bson.M{"$mod": []int{numShards, shardNum}}}
	keys := c.Find(q).Iter()
	res := &KeyVal{}
	for keys.Next(res) {
		if err := c.Refresh(res.Key); err != nil {
			return nil
		}
	}
	if err := keys.Close(); err != nil {
		return err
	}
	return nil
}
