package sqlite

import (
	"sync"
	"unsafe"

	"modernc.org/libc"
	"modernc.org/libc/sys/types"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	ptrSize                 = unsafe.Sizeof(uintptr(0))
	sqliteLockedSharedcache = sqlite3.SQLITE_LOCKED | (1 << 8)
)

type mutex struct {
	sync.Mutex
}

func mutexAlloc(tls *libc.TLS) uintptr {
	return libc.Xcalloc(tls, 1, types.Size_t(unsafe.Sizeof(mutex{})))
}

func mutexFree(tls *libc.TLS, m uintptr) { libc.Xfree(tls, m) }

// Error -
type Error struct {
	msg  string
	code int
}

// Error -
func (e *Error) Error() string { return e.msg }

// Code -
func (e *Error) Code() int { return e.code }
