package sqlite

import (
	"database/sql/driver"
	"sync/atomic"
	"unsafe"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// Stmt -
type Stmt struct {
	c    *Conn
	psql uintptr
}

func newStmt(c *Conn, sql string) (*Stmt, error) {
	p, err := libc.CString(sql)
	if err != nil {
		return nil, err
	}
	stm := Stmt{c: c, psql: p}

	return &stm, nil
}

// Close -
func (s *Stmt) Close() (err error) {
	s.c.free(s.psql)
	s.psql = 0
	return nil
}

// Exec -
func (s *Stmt) Exec(args ...driver.Value) error {
	s.c.Lock()
	defer s.c.Unlock()
	return s.exec(toNamedValues(args...))
}

func (s *Stmt) exec(args []driver.NamedValue) (err error) {
	var pstmt uintptr
	var done int32

	for psql := s.psql; *(*byte)(unsafe.Pointer(psql)) != 0 && atomic.LoadInt32(&done) == 0; {
		if pstmt, err = s.c.prepareV2(&psql); err != nil {
			return err
		}

		if pstmt == 0 {
			continue
		}
		err = func() (err error) {
			n, err := s.c.bindParameterCount(pstmt)
			if err != nil {
				return err
			}

			if n != 0 {
				allocs, err := s.c.bind(pstmt, n, args)
				if err != nil {
					return err
				}

				if len(allocs) != 0 {
					defer func() {
						for _, v := range allocs {
							s.c.free(v)
						}
					}()
				}
			}

			rc, err := s.c.step(pstmt)
			if err != nil {
				return err
			}

			switch rc & 0xff {
			case sqlite3.SQLITE_DONE, sqlite3.SQLITE_ROW:
				// nop
			default:
				return s.c.errstr(int32(rc))
			}

			return nil
		}()

		if e := s.c.finalize(pstmt); e != nil && err == nil {
			err = e
		}

		if err != nil {
			return err
		}
	}
	return nil
}

// NumInput -
func (s *Stmt) NumInput() (n int) {
	return -1
}

// Query -
func (s *Stmt) Query(args ...driver.Value) (*Rows, error) {
	s.c.Lock()
	defer s.c.Unlock()
	return s.query(toNamedValues(args...))
}

func (s *Stmt) query(args []driver.NamedValue) (r *Rows, err error) {
	var pstmt uintptr
	var done int32

	var allocs []uintptr
	for psql := s.psql; *(*byte)(unsafe.Pointer(psql)) != 0 && atomic.LoadInt32(&done) == 0; {
		if pstmt, err = s.c.prepareV2(&psql); err != nil {
			return nil, err
		}

		if pstmt == 0 {
			continue
		}

		err = func() (err error) {
			n, err := s.c.bindParameterCount(pstmt)
			if err != nil {
				return err
			}

			if n != 0 {
				if allocs, err = s.c.bind(pstmt, n, args); err != nil {
					return err
				}
			}

			rc, err := s.c.step(pstmt)
			if err != nil {
				return err
			}

			switch rc & 0xff {
			case sqlite3.SQLITE_ROW:
				if r != nil {
					r.Close()
				}
				if r, err = newRows(s.c, pstmt, allocs, false); err != nil {
					return err
				}

				pstmt = 0
				return nil
			case sqlite3.SQLITE_DONE:
				if r == nil {
					if r, err = newRows(s.c, pstmt, allocs, true); err != nil {
						return err
					}
					pstmt = 0
					return nil
				}

				// nop
			default:
				return s.c.errstr(int32(rc))
			}

			if *(*byte)(unsafe.Pointer(psql)) == 0 {
				if r != nil {
					r.Close()
				}
				if r, err = newRows(s.c, pstmt, allocs, true); err != nil {
					return err
				}

				pstmt = 0
			}
			return nil
		}()
		if e := s.c.finalize(pstmt); e != nil && err == nil {
			err = e
		}

		if err != nil {
			return nil, err
		}
	}
	return r, err
}
