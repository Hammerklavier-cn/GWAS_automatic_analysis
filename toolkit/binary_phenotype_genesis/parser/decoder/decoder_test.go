package decoder

import (
	"log"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"

	filetype "gitcode.com/hammerklavier/GWAS_automatic_analysis/toolkit/binary_phenotype_genesis/parser/FileType"
)

var fid_and_iid_reference = [][2]string{
	{"Family 1", "Person A"},
	{"Family 1", "Person B"},
	{"Family 2", "Person A"},
	{"Family 3", "Person A"},
	{"Family 3", "Person B"},
	{"Family 3", "Person C"},
	{"Family 3", "Person D"},
	{"Family 3", "Person E"},
	{"Family 4", "Person X"},
	{"Family_5", "Person_Z"},
	{"participant01", "participant01"},
	{"participant02", "participant02"},
	{"participant03", "participant03"},
	{"participant04", "participant04"},
	{"participant_infinite", "participant_infinite"},
	{"null", "null"},
	{"nan", "nan"},
	{"participant_quoted", "participant_quoted"},
}

var iid_only_reference = [][2]string{
	{"participant01", "participant01"},
	{"participant02", "participant02"},
	{"participant03", "participant03"},
	{"participant04", "participant04"},
	{"participant_infinite", "participant_infinite"},
	{"null", "null"},
	{"nan", "nan"},
	{"participant_quoted", "participant_quoted"},
}

func TestExtBehaviour(t *testing.T) {
	ext := filepath.Ext("/home/user/myfile.ext")
	if ext != ".ext" {
		t.Fatalf("Ext() generates %v, not .ext\n", ext)
	}
}

func TestCheckColumns(t *testing.T) {
	// IID only
	{
		folder := "../../testfiles/only_iid"
		files, err := os.ReadDir(folder)
		if err != nil {
			t.Fatalf("Failed to read directory: %v\n", err)
		}
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			var file_type filetype.TableType
			if filepath.Ext(file.Name()) == ".csv" {
				file_type = filetype.Csv
			} else {
				file_type = filetype.Tsv
			}
			content_type, err := checkColumns(
				path.Join(folder, file.Name()),
				file_type,
			)
			if err != nil {
				t.Fatal(err)
			}
			if content_type != filetype.IidOnly {
				log.Printf("Wrongly interpret %v as FidAndIid\n", file.Name())
				t.Fail()
			}
		}
	}
	// FID and IID
	{
		folder := "../../testfiles/fid_and_iid"
		files, err := os.ReadDir(folder)
		if err != nil {
			t.Fatalf("Failed to read directory: %v\n", err)
		}
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			var file_type filetype.TableType
			if filepath.Ext(file.Name()) == ".csv" {
				file_type = filetype.Csv
			} else {
				file_type = filetype.Tsv
			}
			content_type, err := checkColumns(
				path.Join(folder, file.Name()),
				file_type,
			)
			if err != nil {
				t.Fatal(err)
			}
			if content_type != filetype.FidAndIid {
				log.Printf("Wrongly interpret %v as IidOnly\n", file.Name())
				t.Fail()
			}
		}
	}
}

func TestParseFile(t *testing.T) {
	var file_path string
	var err error
	var res [][2]string
	// fid and iid are both present
	//
	// no header
	file_path = "../../testfiles/fid_and_iid/all_participants.tsv"

	res, err = ParseFile(
		file_path,
		false,
	)
	if err != nil {
		t.Fatalf("Failed to parse %v: %v\n", file_path, err)
	}
	if !reflect.DeepEqual(res, fid_and_iid_reference) {
		t.Fatalf("Expected %v, but got %v", fid_and_iid_reference, res)
	}

	// with header, csv
	file_path = "../../testfiles/fid_and_iid/all_participants_with_header.csv"

	res, err = ParseFile(
		file_path,
		true,
	)
	if err != nil {
		t.Fatalf("Failed to parse %v: %v\n", file_path, err)
	}
	if !reflect.DeepEqual(res, fid_and_iid_reference) {
		t.Fatalf("Expected %v, but got %v", fid_and_iid_reference, res)
	}

	// only iid present
	file_path = "../../testfiles/only_iid/example_all_participants_iid"
	res, err = ParseFile(file_path, false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, iid_only_reference) {
		t.Fatalf("Expected %v, but got %v", iid_only_reference, res)
	}

	// Parse large file
	file_path = "../../testfiles/examples/E78 Disorders of lipoprotein metabolism and other lipidaemias.csv"
	_, err = ParseFile(file_path, true)
	if err != nil {
		t.Fatal(err)
	}
}
