package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"unicode"

	_ "github.com/mattn/go-sqlite3"
)

var (
	HeadersOn      = true
	Replacing      = false
	WritePlainText = false
)

func main() {
	db, err := sql.Open("sqlite3", "file::memory:")
	if err != nil {
		log.Fatalf("error opening in-memory database: %v", err)
	}
	defer db.Close()

	tableName := ""
	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		switch {
		case arg == "--plain-text" || arg == "--plain" || arg == "--text":
			WritePlainText = true
		case arg == "--replace":
			Replacing = true
		case arg == "--db":
			if i+1 >= len(os.Args) {
				log.Fatalf("--db requires an argument")
			}
			i++
			dbName := os.Args[i]
			if !strings.HasSuffix(dbName, ".db") {
				dbName += ".db"
			}
			db, err = sql.Open("sqlite3", dbName)
			if err != nil {
				log.Fatalf("error opening database %s: %v", os.Args[i], err)
			}
		case arg == "--table":
			if i+1 >= len(os.Args) {
				log.Fatalf("--table requires an argument")
			}
			i++
			tableName = os.Args[i]
		case arg == "--no-headers" || arg == "--no-header":
			HeadersOn = false
		case arg == "--headers":
			HeadersOn = true
		case strings.HasSuffix(arg, ".csv"):
			fileName := arg
			if tableName == "" {
				tableName = sqlNameFromString(fileName)
			}
			createTableFromCSV(db, tableName, fileName)
			tableName = "" // reset table name
		case strings.HasSuffix(arg, ".sql"):
			query := readSQLFile(arg)
			executeQuery(db, query)
		case arg == "stdin":
			if tableName == "" {
				tableName = "stdin"
			}
			createTableFromReader(db, tableName, os.Stdin, "stdin")
		case strings.HasPrefix(arg, "select"):
			executeQuery(db, arg)
		default:
			log.Fatalf("unknown argument: %s", arg)
		}
	}
}

func readSQLFile(fileName string) string {
	contents, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("error reading SQL file %s: %v", fileName, err)
	}
	return string(contents)
}

func executeQuery(db *sql.DB, query string) {

	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("error executing query %#v database: %v", query, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("error getting columns from query %#v: %v", query, err)
	}

	values := make([]sql.NullString, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := 0; i < len(columns); i++ {
		valuePtrs[i] = &values[i]
	}

	writer := getWriter()
	defer writer.Flush()

	if HeadersOn {
		writer.Write(columns)
	}

	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			log.Fatalf("error scanning rows: %v", err)
		}
		writer.Write(asStrings(values))
	}
}

func getWriter() OutputWriter {
	if WritePlainText {
		return NewPlainTextWriter(os.Stdout)
	}
	return NewCSVWriter(os.Stdout)
}

type OutputWriter interface {
	Write([]string) error
	Flush()
}

type CSVWriter struct {
	*csv.Writer
}

func NewCSVWriter(w io.Writer) *CSVWriter {
	return &CSVWriter{csv.NewWriter(w)}
}

func (w *CSVWriter) Write(ss []string) error {
	return w.Writer.Write(ss)
}

func (w *CSVWriter) Flush() {
	w.Writer.Flush()
}

type PlainTextWriter struct {
	*bufio.Writer
}

func NewPlainTextWriter(w io.Writer) *PlainTextWriter {
	return &PlainTextWriter{
		Writer: bufio.NewWriter(w),
	}
}

func (w *PlainTextWriter) Write(ss []string) error {
	fmt.Fprintln(w.Writer, strings.Join(ss, "\t"))
	return nil
}

func (w *PlainTextWriter) Flush() {
	w.Writer.Flush()
}

func asStrings(ss []sql.NullString) []string {
	result := make([]string, len(ss))
	for i, s := range ss {
		result[i] = s.String
	}
	return result
}

func sqlNameFromString(path string) string {
	path = strings.TrimSuffix(path, ".csv")
	// get the last part of the path
	parts := strings.Split(path, "/")
	path = parts[len(parts)-1]
	// replace all non-alphanumeric characters with underscores
	path = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return r
		}
		return '_'
	}, path)
	return path
}

func createTableFromCSV(db *sql.DB, tableName, fileName string) {

	csvFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("error opening the CSV file %s: %v", fileName, err)
	}
	defer csvFile.Close()

	createTableFromReader(db, tableName, csvFile, fileName)
}

func createTableFromReader(db *sql.DB, tableName string, input io.Reader, fileName string) {

	reader := csv.NewReader(input)
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("failed to read all of CSV from %s: %v", fileName, err)
	}

	fieldNames := mapStrings(records[0], sqlNameFromString)
	fieldCount := len(fieldNames)

	if Replacing {
		_, err = db.Exec(fmt.Sprintf("drop table if exists %s", tableName))
		if err != nil {
			log.Fatalf("error dropping table %s: %v", tableName, err)
		}
	}

	createTableStmt := fmt.Sprintf("create table %s (%s)", tableName, strings.Join(fieldNames, ", "))
	_, err = db.Exec(createTableStmt)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			log.Printf("table %s already exists", tableName)
		} else {
			log.Fatalf("error creating table %s: %v", tableName, err)
		}
	}

	insertStmt := fmt.Sprintf("insert into %s (%s) values (%s)", tableName,
		strings.Join(fieldNames, ", "),
		strings.TrimRight(strings.Repeat("?, ", fieldCount), ", "))

	for _, record := range records[1:] {
		values := make([]interface{}, 0, fieldCount)
		for _, v := range record {
			values = append(values, v)
		}
		for i := len(record); i < fieldCount; i++ {
			values = append(values, nil)
		}
		_, err := db.Exec(insertStmt, values...)
		if err != nil {
			log.Fatalf("error inserting record: %v", err)
		}
	}
}

func mapStrings(ss []string, f func(string) string) []string {
	result := make([]string, len(ss))
	for i, s := range ss {
		result[i] = f(s)
	}
	return result
}
