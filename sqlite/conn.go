package sqlite

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"sync"
	"unsafe"

	"github.com/bendersilver/glog"
	"modernc.org/libc"
	"modernc.org/libc/sys/types"
	sqlite3 "modernc.org/sqlite/lib"
)

func (c *Conn) columnBlob(pstmt uintptr, iCol int) (v []byte, err error) {
	p := sqlite3.Xsqlite3_column_blob(c.tls, pstmt, int32(iCol))
	len, err := c.columnBytes(pstmt, iCol)
	if err != nil {
		return nil, err
	}

	if p == 0 || len == 0 {
		return nil, nil
	}

	v = make([]byte, len)
	copy(v, (*libc.RawMem)(unsafe.Pointer(p))[:len:len])
	return v, nil
}

func (c *Conn) columnBytes(pstmt uintptr, iCol int) (_ int, err error) {
	v := sqlite3.Xsqlite3_column_bytes(c.tls, pstmt, int32(iCol))
	return int(v), nil
}

func (c *Conn) columnText(pstmt uintptr, iCol int) (v string, err error) {
	p := sqlite3.Xsqlite3_column_text(c.tls, pstmt, int32(iCol))
	len, err := c.columnBytes(pstmt, iCol)
	if err != nil {
		return "", err
	}

	if p == 0 || len == 0 {
		return "", nil
	}

	b := make([]byte, len)
	copy(b, (*libc.RawMem)(unsafe.Pointer(p))[:len:len])
	return string(b), nil
}

func (c *Conn) columnDouble(pstmt uintptr, iCol int) (v float64, err error) {
	v = sqlite3.Xsqlite3_column_double(c.tls, pstmt, int32(iCol))
	return v, nil
}

func (c *Conn) columnInt64(pstmt uintptr, iCol int) (v int64, err error) {
	v = sqlite3.Xsqlite3_column_int64(c.tls, pstmt, int32(iCol))
	return v, nil
}

func (c *Conn) columnType(pstmt uintptr, iCol int) (_ int, err error) {
	v := sqlite3.Xsqlite3_column_type(c.tls, pstmt, int32(iCol))
	return int(v), nil
}

func (c *Conn) columnDeclType(pstmt uintptr, iCol int) string {
	return libc.GoString(sqlite3.Xsqlite3_column_decltype(c.tls, pstmt, int32(iCol)))
}

func (c *Conn) columnName(pstmt uintptr, n int) (string, error) {
	p := sqlite3.Xsqlite3_column_name(c.tls, pstmt, int32(n))
	return libc.GoString(p), nil
}

func (c *Conn) columnCount(pstmt uintptr) (_ int, err error) {
	v := sqlite3.Xsqlite3_column_count(c.tls, pstmt)
	return int(v), nil
}

func (c *Conn) step(pstmt uintptr) (int, error) {
	for {
		switch rc := sqlite3.Xsqlite3_step(c.tls, pstmt); rc {
		case sqliteLockedSharedcache:
			if err := c.retry(pstmt); err != nil {
				return sqlite3.SQLITE_LOCKED, err
			}
		case
			sqlite3.SQLITE_DONE,
			sqlite3.SQLITE_ROW:

			return int(rc), nil
		default:
			return int(rc), c.errstr(rc)
		}
	}
}

func (c *Conn) retry(pstmt uintptr) error {
	mu := mutexAlloc(c.tls)
	(*mutex)(unsafe.Pointer(mu)).Lock()
	rc := sqlite3.Xsqlite3_unlock_notify(
		c.tls,
		c.db,
		*(*uintptr)(unsafe.Pointer(&struct {
			f func(*libc.TLS, uintptr, int32)
		}{unlockNotify})),
		mu,
	)
	if rc == sqlite3.SQLITE_LOCKED {
		(*mutex)(unsafe.Pointer(mu)).Unlock()
		mutexFree(c.tls, mu)
		return c.errstr(rc)
	}

	(*mutex)(unsafe.Pointer(mu)).Lock()
	(*mutex)(unsafe.Pointer(mu)).Unlock()
	mutexFree(c.tls, mu)
	if pstmt != 0 {
		sqlite3.Xsqlite3_reset(c.tls, pstmt)
	}
	return nil
}

func unlockNotify(t *libc.TLS, ppArg uintptr, nArg int32) {
	for i := int32(0); i < nArg; i++ {
		mu := *(*uintptr)(unsafe.Pointer(ppArg))
		(*mutex)(unsafe.Pointer(mu)).Unlock()
		ppArg += ptrSize
	}
}

func (c *Conn) bind(pstmt uintptr, n int, args []driver.NamedValue) (allocs []uintptr, err error) {
	defer func() {
		if err == nil {
			return
		}

		for _, v := range allocs {
			c.free(v)
		}
		allocs = nil
	}()

	for i := 1; i <= n; i++ {
		name, err := c.bindParameterName(pstmt, i)
		if err != nil {
			return allocs, err
		}

		var found bool
		var v driver.NamedValue
		for _, v = range args {
			if name != "" {
				// For ?NNN and $NNN params, match if NNN == v.Ordinal.
				//
				// Supporting this for $NNN is a special case that makes eg
				// `select $1, $2, $3 ...` work without needing to use
				// sql.Named.
				if (name[0] == '?' || name[0] == '$') && name[1:] == strconv.Itoa(v.Ordinal) {
					found = true
					break
				}

				// sqlite supports '$', '@' and ':' prefixes for string
				// identifiers and '?' for numeric, so we cannot
				// combine different prefixes with the same name
				// because `database/sql` requires variable names
				// to start with a letter
				if name[1:] == v.Name[:] {
					found = true
					break
				}
			} else {
				if v.Ordinal == i {
					found = true
					break
				}
			}
		}

		if !found {
			if name != "" {
				return allocs, fmt.Errorf("missing named argument %q", name[1:])
			}

			return allocs, fmt.Errorf("missing argument with index %d", i)
		}

		var p uintptr
		switch x := v.Value.(type) {
		case int64:
			if err := c.bindInt64(pstmt, i, x); err != nil {
				return allocs, err
			}
		case float64:
			if err := c.bindDouble(pstmt, i, x); err != nil {
				return allocs, err
			}
		case bool:
			v := 0
			if x {
				v = 1
			}
			if err := c.bindInt(pstmt, i, v); err != nil {
				return allocs, err
			}
		case []byte:
			if p, err = c.bindBlob(pstmt, i, x); err != nil {
				return allocs, err
			}
		case string:
			if p, err = c.bindText(pstmt, i, x); err != nil {
				return allocs, err
			}
		case nil:
			if p, err = c.bindNull(pstmt, i); err != nil {
				return allocs, err
			}
		default:
			glog.Debug(x)
			return allocs, fmt.Errorf("sqlite: invalid driver.Value type %T", x)
		}
		if p != 0 {
			allocs = append(allocs, p)
		}
	}
	return allocs, nil
}

func (c *Conn) bindNull(pstmt uintptr, idx1 int) (uintptr, error) {
	if rc := sqlite3.Xsqlite3_bind_null(c.tls, pstmt, int32(idx1)); rc != sqlite3.SQLITE_OK {
		return 0, c.errstr(rc)
	}

	return 0, nil
}

func (c *Conn) bindText(pstmt uintptr, idx1 int, value string) (uintptr, error) {
	p, err := libc.CString(value)
	if err != nil {
		return 0, err
	}

	if rc := sqlite3.Xsqlite3_bind_text(c.tls, pstmt, int32(idx1), p, int32(len(value)), 0); rc != sqlite3.SQLITE_OK {
		c.free(p)
		return 0, c.errstr(rc)
	}

	return p, nil
}
func (c *Conn) bindBlob(pstmt uintptr, idx1 int, value []byte) (uintptr, error) {
	if value != nil && len(value) == 0 {
		if rc := sqlite3.Xsqlite3_bind_zeroblob(c.tls, pstmt, int32(idx1), 0); rc != sqlite3.SQLITE_OK {
			return 0, c.errstr(rc)
		}
		return 0, nil
	}

	p, err := c.malloc(len(value))
	if err != nil {
		return 0, err
	}
	if len(value) != 0 {
		copy((*libc.RawMem)(unsafe.Pointer(p))[:len(value):len(value)], value)
	}
	if rc := sqlite3.Xsqlite3_bind_blob(c.tls, pstmt, int32(idx1), p, int32(len(value)), 0); rc != sqlite3.SQLITE_OK {
		c.free(p)
		return 0, c.errstr(rc)
	}

	return p, nil
}

func (c *Conn) bindInt(pstmt uintptr, idx1, value int) (err error) {
	if rc := sqlite3.Xsqlite3_bind_int(c.tls, pstmt, int32(idx1), int32(value)); rc != sqlite3.SQLITE_OK {
		return c.errstr(rc)
	}

	return nil
}

func (c *Conn) bindDouble(pstmt uintptr, idx1 int, value float64) (err error) {
	if rc := sqlite3.Xsqlite3_bind_double(c.tls, pstmt, int32(idx1), value); rc != 0 {
		return c.errstr(rc)
	}

	return nil
}

func (c *Conn) bindInt64(pstmt uintptr, idx1 int, value int64) (err error) {
	if rc := sqlite3.Xsqlite3_bind_int64(c.tls, pstmt, int32(idx1), value); rc != sqlite3.SQLITE_OK {
		return c.errstr(rc)
	}

	return nil
}

func (c *Conn) bindParameterName(pstmt uintptr, i int) (string, error) {
	p := sqlite3.Xsqlite3_bind_parameter_name(c.tls, pstmt, int32(i))
	return libc.GoString(p), nil
}

func (c *Conn) bindParameterCount(pstmt uintptr) (_ int, err error) {
	r := sqlite3.Xsqlite3_bind_parameter_count(c.tls, pstmt)
	return int(r), nil
}

func (c *Conn) finalize(pstmt uintptr) error {
	if rc := sqlite3.Xsqlite3_finalize(c.tls, pstmt); rc != sqlite3.SQLITE_OK {
		return c.errstr(rc)
	}

	return nil
}

func (c *Conn) prepareV2(zSQL *uintptr) (pstmt uintptr, err error) {
	var ppstmt, pptail uintptr

	defer func() {
		c.free(ppstmt)
		c.free(pptail)
	}()

	if ppstmt, err = c.malloc(int(ptrSize)); err != nil {
		return 0, err
	}

	if pptail, err = c.malloc(int(ptrSize)); err != nil {
		return 0, err
	}

	for {
		switch rc := sqlite3.Xsqlite3_prepare_v2(c.tls, c.db, *zSQL, -1, ppstmt, pptail); rc {
		case sqlite3.SQLITE_OK:
			*zSQL = *(*uintptr)(unsafe.Pointer(pptail))
			return *(*uintptr)(unsafe.Pointer(ppstmt)), nil
		case sqliteLockedSharedcache:
			if err := c.retry(0); err != nil {
				return 0, err
			}
		default:
			return 0, c.errstr(rc)
		}
	}
}

func (c *Conn) interrupt(pdb uintptr) (err error) {
	c.Lock()
	defer c.Unlock()

	if c.tls != nil {
		sqlite3.Xsqlite3_interrupt(c.tls, pdb)
	}
	return nil
}

func (c *Conn) extendedResultCodes(on bool) error {
	if rc := sqlite3.Xsqlite3_extended_result_codes(c.tls, c.db, libc.Bool32(on)); rc != sqlite3.SQLITE_OK {
		return c.errstr(rc)
	}
	return nil
}

func (c *Conn) openV2(name string, flags int32) (uintptr, error) {
	var p, s uintptr

	defer func() {
		if p != 0 {
			c.free(p)
		}
		if s != 0 {
			c.free(s)
		}
	}()

	p, err := c.malloc(int(ptrSize))
	if err != nil {
		return 0, err
	}

	if s, err = libc.CString(name); err != nil {
		return 0, err
	}

	if rc := sqlite3.Xsqlite3_open_v2(c.tls, s, p, flags, 0); rc != sqlite3.SQLITE_OK {
		return 0, c.errstr(rc)
	}

	return *(*uintptr)(unsafe.Pointer(p)), nil
}

func (c *Conn) malloc(n int) (uintptr, error) {
	if p := libc.Xmalloc(c.tls, types.Size_t(n)); p != 0 || n == 0 {
		return p, nil
	}

	return 0, fmt.Errorf("sqlite: cannot allocate %d bytes of memory", n)
}

func (c *Conn) free(p uintptr) {
	if p != 0 {
		libc.Xfree(c.tls, p)
	}
}

func (c *Conn) errstr(rc int32) error {
	p := sqlite3.Xsqlite3_errstr(c.tls, rc)
	str := libc.GoString(p)
	p = sqlite3.Xsqlite3_errmsg(c.tls, c.db)
	var s string
	if rc == sqlite3.SQLITE_BUSY {
		s = " (SQLITE_BUSY)"
	}
	switch msg := libc.GoString(p); {
	case msg == str:
		return &Error{msg: fmt.Sprintf("%s (%v)%s", str, rc, s), code: int(rc)}
	default:
		return &Error{msg: fmt.Sprintf("%s: %s (%v)%s", str, msg, rc, s), code: int(rc)}
	}
}

func (c *Conn) Exec(query string, args ...driver.Value) error {
	return c.exec(query, toNamedValues(args...))
}

func (c *Conn) exec(query string, args []driver.NamedValue) error {
	s, err := c.prepare(query)
	if err != nil {
		return err
	}

	defer func() {
		if err2 := s.Close(); err2 != nil && err == nil {
			err = err2
		}
	}()

	return s.exec(args)
}

func (c *Conn) Prepare(query string) (*Stmt, error) {
	return c.prepare(query)
}

func (c *Conn) prepare(query string) (*Stmt, error) {
	return newStmt(c, query)
}

func (c *Conn) Query(query string, args ...driver.Value) (*Rows, error) {
	return c.query(query, toNamedValues(args...))
}

func (c *Conn) query(query string, args []driver.NamedValue) (r *Rows, err error) {
	s, err := c.prepare(query)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err2 := s.Close(); err2 != nil && err == nil {
			err = err2
		}
	}()

	return s.query(args)
}

func (c *Conn) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.db != 0 {
		if err := c.closeV2(c.db); err != nil {
			return err
		}
		c.db = 0
	}

	if c.tls != nil {
		c.tls.Close()
		c.tls = nil
	}
	return nil
}

func (c *Conn) closeV2(db uintptr) error {
	if rc := sqlite3.Xsqlite3_close_v2(c.tls, db); rc != sqlite3.SQLITE_OK {
		return c.errstr(rc)
	}

	return nil
}

type Conn struct {
	db  uintptr
	tls *libc.TLS
	sync.Mutex
	writeTimeFormat string
	beginMode       string
}

func newConn() (*Conn, error) {
	c := &Conn{tls: libc.NewTLS()}
	db, err := c.openV2(
		"file:redispg?mode=memory",
		sqlite3.SQLITE_OPEN_READWRITE|
			sqlite3.SQLITE_OPEN_CREATE|
			sqlite3.SQLITE_OPEN_FULLMUTEX|
			sqlite3.SQLITE_OPEN_URI|
			sqlite3.SQLITE_OPEN_SHAREDCACHE,
	)
	if err != nil {
		return nil, err
	}

	c.db = db
	if err = c.extendedResultCodes(true); err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

func NewMemConn() (*Conn, error) {
	return newConn()
}
