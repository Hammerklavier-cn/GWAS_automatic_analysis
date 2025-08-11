package decoder

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	filetype "gitcode.com/hammerklavier/GWAS_automatic_analysis/toolkit/binary_phenotype_genesis/parser/FileType"
)

type ParsedResult struct {
	Phenotype    string
	Participants [][2]string
}

func checkColumns(file_path string, file_type filetype.TableType) (filetype.ContentType, error) {

	file, err := os.Open(file_path)
	if err != nil {
		return filetype.IidOnly, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	if scanner.Scan() {
		line := scanner.Text()

		var column_count = len(strings.Split(line, file_type.Separator()))

		switch column_count {
		case 1:
			return filetype.IidOnly, nil
		case 2:
			return filetype.FidAndIid, nil
		default:
			return filetype.Unknown, fmt.Errorf("Unexpected column count: %d\n", column_count)
		}
	} else {
		return filetype.Unknown, fmt.Errorf("Failed to interpret column numbers\n")
	}
}

func ParseFile(file_path string, has_header bool) ([][2]string, error) {
	var file_type filetype.TableType
	if filepath.Ext(file_path) == ".csv" {
		file_type = filetype.Csv
	} else {
		file_type = filetype.Tsv
	}

	content_type, err := checkColumns(file_path, file_type)
	if err != nil {
		return nil, err
	}

	var results [][2]string

	file_io, err := os.Open(file_path)
	if err != nil {
		return nil, err
	}
	defer file_io.Close()

	scanner := bufio.NewScanner(file_io)
	// Trim first line
	if has_header {
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return nil, fmt.Errorf(
					"Failed to read file %v: %v\n",
					file_path, err,
				)
			} else {
				return nil, nil
			}
		}
	}

	// Parse
	for i := 0; scanner.Scan(); i++ {
		line := scanner.Text()
		line = strings.Trim(line, " \t\n")
		if line == "" {
			continue
		}

		switch content_type {
		case filetype.IidOnly:
			line = strings.Trim(line, ` '"`+"\t")
			results = append(results, [2]string{line, line})

		case filetype.FidAndIid:
			columns := strings.Split(line, file_type.Separator())
			if len(columns) < 2 {
				return nil, fmt.Errorf(
					"Failed to parse %v at line %v: Less than 2 columns detected\nline: '%v'\nSep: '%v'\n",
					file_path, i, line, file_type.Separator(),
				)
			}
			fid := strings.Trim(columns[0], ` '"`+"\t")
			iid := strings.Trim(columns[1], ` '"`+"\t")
			results = append(results, [2]string{fid, iid})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return results, nil
}
