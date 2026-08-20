package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck   kvtest.IKVClerk
	key  string
	myID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:   ck,
		key:  lockname,
		myID: kvtest.RandValue(8)}

	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			err2 := lk.ck.Put(lk.key, lk.myID, 0)
			if err2 == rpc.OK {
				break
			}
		} else if value == "" {
			err2 := lk.ck.Put(lk.key, lk.myID, version)
			if err2 == rpc.OK {
				break
			}
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (lk *Lock) Release() {
	value, version, err := lk.ck.Get(lk.key)
	if err != rpc.OK {
		panic("Get failed")
	}
	if value != lk.myID {
		panic("Release failed: not owner")
	}
	err2 := lk.ck.Put(lk.key, "", version)
	if err2 != rpc.OK {
		panic("Release failed")
	}
}
