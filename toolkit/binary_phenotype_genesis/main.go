package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"gitcode.com/hammerklavier/GWAS_automatic_analysis/toolkit/binary_phenotype_genesis/cli"
	"gitcode.com/hammerklavier/GWAS_automatic_analysis/toolkit/binary_phenotype_genesis/parser/decoder"
	"gitcode.com/hammerklavier/GWAS_automatic_analysis/toolkit/binary_phenotype_genesis/parser/encoder"
)

func main() {
	if err := cli.Execute(); err != nil {
		log.Fatalln("Failed to parse command line arguments: ", err)
	}

	if len(os.Args) > 1 {
		for _, arg := range os.Args[1:] {
			exit_map := make(map[string]struct{}, 4)
			exit_map["-h"] = struct{}{}
			exit_map["--help"] = struct{}{}
			exit_map["-v"] = struct{}{}
			exit_map["--version"] = struct{}{}
			if _, ok := exit_map[arg]; ok {
				os.Exit(0)
			}
		}
	}

	fmt.Println("Read from file(s)", cli.InputFiles, " and folder ", cli.InputFolder)

	if !(cli.OutputSingleFile || cli.OutputMultipleFiles) {
		cli.OutputSingleFile = true
	}
	if cli.OutputSingleFile {
		log.Println("Export results to single file. File will be saved at ", cli.OutputPrefix)
	} else if cli.OutputMultipleFiles {
		log.Println("Export results to multiple files. Files will be saved under ", cli.OutputPrefix)
	} else {
		log.Fatalln(
			"Impossible! Cannot determine whether to save to single file or multiple files.",
		)
	}

	// Check files' existence
	all_input_files := make(map[string]bool)

	if cli.InputFolder != "" {
		if info, err := os.Stat(cli.InputFolder); err != nil {
			log.Fatalln(
				"Failed to access ", cli.InputFolder, " : ", err,
			)
		} else if !info.IsDir() {
			log.Fatalln(cli.InputFolder, " is not a directory")
		}
		files, err := os.ReadDir(cli.InputFolder)
		if err != nil {
			log.Fatalln("Failed to read directory:", err)
		}

		for _, file := range files {
			if !file.IsDir() {
				file_path := path.Join(cli.InputFolder, file.Name())
				log.Println("Found file:", file_path)
				all_input_files[file_path] = true
			}
		}
	}

	if cli.InputFiles != nil {
		for _, file_path := range cli.InputFiles {
			if file_info, err := os.Stat(file_path); err != nil {
				log.Fatalf(
					"Failed to access %v : %v\n",
					file_path, err,
				)
			} else if file_info.IsDir() {
				log.Fatalf("%v is a directory, not a file\n", file_path)
			} else {
				log.Println("Found file:", file_path)
				all_input_files[file_path] = true
			}
		}
	}

	if fileinfo, err := os.Stat(cli.AllParticipantsFile); err != nil {
		log.Fatalf("%v not found\n", cli.AllParticipantsFile)
	} else if fileinfo.IsDir() {
		log.Fatalf("%v is a directory, not a file\n", cli.AllParticipantsFile)
	} else {
		log.Printf("Found all participants' file: %s\n", cli.AllParticipantsFile)
	}

	// Parse and export
	// File of all participants
	log.Printf("Parsing %v..\n", cli.AllParticipantsFile)
	all_participants, err := decoder.ParseFile(cli.AllParticipantsFile, !cli.InputFilesHaveNoHeader)
	if err != nil {
		log.Fatalf("Failed to parse %s: %v", cli.AllParticipantsFile, err)
	}

	// File of positive participants
	var parse_results []decoder.ParsedResult
	for file_path, _ := range all_input_files {
		log.Printf("Parsing %v...\n", file_path)
		res, err := decoder.ParseFile(file_path, !cli.InputFilesHaveNoHeader)
		if err != nil {
			log.Printf("Failed to parse %v: %v\n", file_path, err)
			continue
		} else if res == nil {
			log.Printf("Parsing %v returns no value\n", file_path)
			continue
		}

		phenotype_name := strings.TrimSuffix(filepath.Base(file_path), filepath.Ext(file_path))
		if !cli.NoReplaceSpaceByUnderline {
			phenotype_name = strings.ReplaceAll(phenotype_name, " ", "_")
		}

		parse_results = append(parse_results, decoder.ParsedResult{Phenotype: phenotype_name, Participants: res})
	}

	// If export to multiple files...
	if cli.OutputMultipleFiles {
		log.Println("Exporting results to multiple files...")
		for _, parse_result := range parse_results {
			participants_and_phenotype := encoder.GenerateSingle(
				all_participants,
				parse_result.Participants,
			)
			if err := encoder.WriteSingleToFile(
				participants_and_phenotype,
				filepath.Join(cli.OutputPrefix, parse_result.Phenotype),
			); err != nil {
				log.Fatalf("%v\n", err)
			}
		}
		// If export to only one file...
	} else if cli.OutputSingleFile {
		log.Println("Exporting results to a single file...")
		phenotype_and_participants_map := make(map[string][][2]string, len(parse_results))
		for _, parse_result := range parse_results {
			phenotype_and_participants_map[parse_result.Phenotype] = parse_result.Participants
		}

		pheno_names, participans_and_phenotypes := encoder.ConvertMultipleToSingle(
			all_participants,
			phenotype_and_participants_map,
		)
		if err := encoder.WriteMultipleToFile(
			cli.OutputPrefix+".tsv",
			pheno_names,
			participans_and_phenotypes...,
		); err != nil {
			log.Fatalf(
				"Failed to write to a single file: %v", err,
			)
		}
	}
	log.Println("Done.")
}
