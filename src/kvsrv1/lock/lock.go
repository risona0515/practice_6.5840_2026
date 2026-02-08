package lock

import (
	"log"
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
	lockname    string
	lockversion rpc.Tversion
}

// var LockMap map[string]*Lock

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	cnt := 0
	for true {
		cnt++
		value, version, err := lk.ck.Get(lk.lockname)
		// if cnt%100 == 2 {
		// 	log.Printf("acquire lock, k %v, v %v, version %v, err %v", lk.lockname, value, version, err)
		// }

		if err == rpc.ErrNoKey {
			version = 0
			err = lk.ck.Put(lk.lockname, "lock", version)
			if err == rpc.OK {
				lk.lockversion = version
				return
			}
		} else if value == "unlock" {
			err = lk.ck.Put(lk.lockname, "lock", version)
			// if err != rpc.OK {
			// 	log.Printf()
			// }
			if err == rpc.OK {
				lk.lockversion = version
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	value, version, err := lk.ck.Get((lk.lockname))
	if err == rpc.ErrNoKey {
		log.Printf("lockname %v tried to unlock but no lock exists", lk.lockname)
	}
	if value == "unlock" {
		log.Printf("lockname %v tried to unlock but already unlocked, stored version %v, get version %v", lk.lockname, lk.lockversion, version)
		return
	}
	if version != lk.lockversion+1 {
		log.Printf("lockname %v tried to unlock but version not match, stored version %v, get version %v", lk.lockname, lk.lockversion, version)
		return
	}
	err = lk.ck.Put(lk.lockname, "unlock", version)
	if err != rpc.OK {
		log.Printf("lockname %v unlock failed, stored version %v, get version %v, err %v", lk.lockname, lk.lockversion, version, err)
	}
}
