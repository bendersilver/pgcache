package pgcache

import (
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

func init() {
	AddCommand(
		&Command{
			Usage:    "QueryRow shema.table_name [params] [values...]",
			Name:     "QueryRow",
			Flags:    "readonly blocking",
			FirstKey: 1, LastKey: 1, KeyStep: 1,
			Arity: -2,
			Action: func(conn redcon.Conn, cmd redcon.Command) (err error) {
				if len(cmd.Args) < 2 {
					return wrongArity
				} else if len(cmd.Args) == 2 {
					err = writeJsonRow(
						conn,
						fmt.Sprintf(`SELECT * FROM %s`, tableName(cmd.Args[1])),
					)
				} else {
					if len(cmd.Args) < 4 {
						return wrongArity
					}
					err = writeJsonRow(
						conn,
						fmt.Sprintf(`SELECT * FROM %s WHERE %s`,
							tableName(cmd.Args[1]),
							cmd.Args[2],
						),
						cmd.Args[3:]...,
					)
				}
				return err
			},
		},
		&Command{
			Usage:    "QueryRow.Fld shema.table_name [col1,col2...] [params] [values...]",
			Name:     "QueryRow.Fld",
			Flags:    "readonly blocking",
			FirstKey: 1, LastKey: 1, KeyStep: 1,
			Arity: -2,
			Action: func(conn redcon.Conn, cmd redcon.Command) (err error) {
				if len(cmd.Args) < 3 {
					return wrongArity
				} else if len(cmd.Args) == 3 {
					err = writeJsonRow(
						conn,
						fmt.Sprintf(
							`SELECT %s FROM %s`, cmd.Args[2],
							tableName(cmd.Args[2]),
						),
					)
				} else {
					if len(cmd.Args) < 5 {
						return wrongArity
					}
					err = writeJsonRow(
						conn,
						fmt.Sprintf(`SELECT %s FROM %s WHERE %s`,
							cmd.Args[1],
							tableName(cmd.Args[2]),
							cmd.Args[3],
						),
						cmd.Args[4:]...,
					)
				}
				return err
			},
		},
		&Command{
			Usage:    "Query shema.table_name [params] [values...]",
			Name:     "Query",
			Flags:    "readonly blocking",
			FirstKey: 1, LastKey: 1, KeyStep: 1,
			Arity: -2,
			Action: func(conn redcon.Conn, cmd redcon.Command) (err error) {
				if len(cmd.Args) < 2 {
					return wrongArity
				} else if len(cmd.Args) == 2 {
					err = writeJsonRows(
						conn,
						fmt.Sprintf(`SELECT * FROM %s`, tableName(cmd.Args[1])),
					)
				} else {
					if len(cmd.Args) < 4 {
						return wrongArity
					}
					err = writeJsonRows(
						conn,
						fmt.Sprintf(`SELECT * FROM %s WHERE %s`,
							tableName(cmd.Args[1]),
							cmd.Args[2],
						),
						cmd.Args[3:]...,
					)
				}
				return err
			},
		},
		&Command{
			Usage:    "Query.Fld shema.table_name [col1,col2...] [params] [values...]",
			Name:     "Query.Fld",
			Flags:    "readonly blocking",
			FirstKey: 1, LastKey: 1, KeyStep: 1,
			Arity: -2,
			Action: func(conn redcon.Conn, cmd redcon.Command) (err error) {
				if len(cmd.Args) < 3 {
					return wrongArity
				} else if len(cmd.Args) == 3 {
					err = writeJsonRows(
						conn,
						fmt.Sprintf(`SELECT %s FROM %s`, cmd.Args[2],
							tableName(cmd.Args[2]),
						),
					)
				} else {
					if len(cmd.Args) < 5 {
						return wrongArity
					}
					err = writeJsonRows(
						conn,
						fmt.Sprintf(`SELECT %s FROM %s WHERE %s`,
							cmd.Args[1],
							tableName(cmd.Args[2]),
							cmd.Args[3],
						),
						cmd.Args[4:]...,
					)
				}
				return err
			},
		},
		&Command{
			Usage:    "RawQuery sql [values...]",
			Name:     "RawQuery",
			Flags:    "readonly blocking",
			FirstKey: 1, LastKey: 1, KeyStep: 1,
			Arity: -2,
			Action: func(conn redcon.Conn, cmd redcon.Command) (err error) {
				if len(cmd.Args) < 2 {
					return wrongArity
				} else if len(cmd.Args) == 2 {
					err = writeJsonRows(conn, fmt.Sprintf(`%s`, cmd.Args[1]), nil)
				} else {
					err = writeJsonRows(
						conn,
						fmt.Sprintf(`%s`, cmd.Args[1]),
						cmd.Args[2:]...,
					)
				}
				return err
			},
		},
	)
}

func tableName(b []byte) string {
	return strings.ReplaceAll(string(b), ".", "_")
}

func writeJsonRow(conn redcon.Conn, query string, args ...[]byte) error {
	qs, err := db.QueryRow(query, args...)
	if err != nil {
		return err
	}
	if len(qs.Rows) == 0 {
		conn.WriteNull()
		return nil
	}
	b, err := qs.Json(0)
	if err != nil {
		return err
	}
	conn.WriteBulk(b)
	return nil
}

func writeJsonRows(conn redcon.Conn, query string, args ...[]byte) error {
	qs, err := db.QuerySet(query, args...)
	if err != nil {
		return err
	}
	if len(qs.Rows) == 0 {
		conn.WriteNull()
		return nil
	}
	conn.WriteArray(len(qs.Rows))
	for i := range qs.Rows {
		b, err := qs.Json(i)
		if err != nil {
			return err
		}
		conn.WriteBulk(b)
	}
	return nil
}
