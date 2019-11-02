package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"

	pwriter "github.com/xitongsys/parquet-go/writer"
)

type NameRecordCSV struct {
	RecordID    int    `csv:"record_id"`
	APN         string `csv:"assessors_parcel_number"`
	PSN         string `csv:"parcel_sequence_number"`
	NSN         string `csv:"name_sequence_number"`
	Name        string `csv:"name"`
	ET          string `csv:"name_et_stnd_code"`
	Description string `csv:"name_description_stnd_code"`
	Type        string `csv:"name_type_stnd_code"`
	Pattern     string `csv:"name_pattern_stnd_code"`
	Class       string `csv:"name_class_stnd_code"`
}

type NameRecordParquet struct {
	RecordID    int    `parquet:"name=record_id, type=INT64"`
	APN         string `parquet:"name=assessors_parcel_number, type=UTF8, encoding=PLAIN_DICTIONARY"`
	PSN         string `parquet:"name=parcel_sequence_number, type=UTF8, encoding=PLAIN_DICTIONARY"`
	NSN         string `parquet:"name=name_sequence_number, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Name        string `parquet:"name=name, type=UTF8, encoding=PLAIN_DICTIONARY"`
	ET          string `parquet:"name=name_et_stnd_code, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Description string `parquet:"name=name_description_stnd_code, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Type        string `parquet:"name=name_type_stnd_code, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Pattern     string `parquet:"name=name_pattern_stnd_code, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Class       string `parquet:"name=name_class_stnd_code, type=UTF8, encoding=PLAIN_DICTIONARY"`
}

func main() {
	limit := 5
	skipFirstLine := true
	var err error

	log.Println("Starting...")

	// set up parquet output writer file
	parquetFile, err := local.NewLocalFileWriter("name.parquet")
	if err != nil {
		log.Fatalln("Can't create local file", err)
	}

	// parquet writer:  https://github.com/xitongsys/parquet-go/blob/master/writer/writer.go
	writer, err := writer.NewParquetWriter(parquetFile, new(NameRecordParquet), 4) // 4 = NP  parallel number
	if err != nil {
		log.Fatalln("Can't create parquet writer", err)
	}

	writer.RowGroupSize = 128 * 1024 * 1024 // 128M
	writer.CompressionType = parquet.CompressionCodec_SNAPPY

	// set up csv reader to read file line by line
	csvFile, err := os.Open("name.tsv")
	if err != nil {
		log.Fatalln(err)
	}
	defer csvFile.Close()

	// https://golang.org/pkg/encoding/csv/
	reader := csv.NewReader(csvFile)
	reader.Comma = '\t'        // Use tab-delimited instead of comma for TSV files
	reader.FieldsPerRecord = 0 // 0 means it will discover it from the first line

	log.Println("Reading csv file...")

	var record []string
	for {
		// read one line at a time
		record, err = reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln(err)
		}

		// skip header row
		if skipFirstLine {
			skipFirstLine = false
			continue
		}

		writeToParquet(writer, record)

		// limit number of rows processed
		if limit > 0 {
			limit--
			if limit == 0 {
				break
			}
		}
	}

	if err = writer.WriteStop(); err != nil {
		log.Fatalln("WriteStop error", err)
	}
	parquetFile.Close()

	log.Println("Write Finished")
}

func writeToParquet(writer *pwriter.ParquetWriter, record []string) {
	// unmarshal the string array row from tsv to csv object
	id, _ := strconv.Atoi(record[0]) // convert int to string
	row := NameRecordParquet{
		RecordID:    id,
		APN:         record[1],
		PSN:         record[2],
		NSN:         record[3],
		Name:        record[4],
		ET:          record[5],
		Description: record[6],
		Type:        record[7],
		Pattern:     record[8],
		Class:       record[9],
	}

	log.Println("writing:", row)

	// convert to parquet and append to output file
	if err := writer.Write(row); err != nil {
		log.Fatalln("Write error", err)
	}
}
