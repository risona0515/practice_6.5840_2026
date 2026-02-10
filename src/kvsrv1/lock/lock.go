package lock

import (
	"log"
	"strconv"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	lockid   int
	// lockversion rpc.Tversion
}

// 用于分配id
type lockIdDistributor struct {
	mu     sync.Mutex
	nextid map[string]int
}

var d = lockIdDistributor{nextid: map[string]int{}}

// var mu sync.Mutex
// var nextid = make(map[string]int)

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname}
	// You may add code here
	d.mu.Lock()
	defer d.mu.Unlock()
	id, ok := d.nextid[lockname]
	if !ok {
		lk.lockid = 1
		d.nextid[lockname] = 2
	} else {
		lk.lockid = id
		d.nextid[lockname]++
	}

	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for true {
		value, version, err := lk.ck.Get(lk.lockname)
		// if cnt%100 == 2 {
		// 	log.Printf("acquire lock, k %v, v %v, version %v, err %v", lk.lockname, value, version, err)
		// }

		if err == rpc.ErrNoKey {
			version = 0
			err = lk.ck.Put(lk.lockname, strconv.Itoa(lk.lockid), version)
			if err == rpc.OK {
				return
			}
		} else if value == "0" {
			err = lk.ck.Put(lk.lockname, strconv.Itoa(lk.lockid), version)
			if err == rpc.OK {
				return
			}
		}
		if err == rpc.ErrMaybe {
			value, version, err = lk.ck.Get(lk.lockname)
			if value == strconv.Itoa(lk.lockid) {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	value, version, _ := lk.ck.Get((lk.lockname))
	if value != strconv.Itoa(lk.lockid) {
		log.Printf("lock release or occupied by others %v", value)
	}
	// err = lk.ck.Put(lk.lockname, "0", version)
	lk.ck.Put(lk.lockname, "0", version)
	// if err == rpc.ErrMaybe {
	// 	value, version, err = lk.ck.Get((lk.lockname))

	// }
}
