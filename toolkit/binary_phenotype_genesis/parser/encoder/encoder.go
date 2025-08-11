package encoder

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type ParticipantAndPhenotype struct {
	Fid      string
	Iid      string
	Positive bool
}

type ParticipantAndPhenotypes struct {
	Fid        string
	Iid        string
	Phenotypes []bool
}

type Participant struct {
	Fid string
	Iid string
}

// Create folder recursively if it does not exist
func setUpFolder(folder string) {
	if _, err := os.Stat(folder); os.IsNotExist(err) {
		err = os.MkdirAll(folder, os.ModePerm)
		if err != nil {
			log.Fatalf("Failed to create folder %v: %v\n", folder, err)
		}
	}
}

// This function should be integrated into WriteSingleToSingle
func GenerateSingle(references, positives [][2]string) []ParticipantAndPhenotype {
	// Convert
	var positive_map = make(map[[2]string]struct{}, 1000)
	// var positive_map = set.NewBTreeSet[[2]string]()

	for _, positive := range positives {
		positive_map[positive] = struct{}{}
	}

	var result = make([]ParticipantAndPhenotype, 0, len(references))

	for _, participant := range references {
		if _, exists := positive_map[participant]; exists {
			result = append(
				result,
				ParticipantAndPhenotype{
					Fid:      participant[0],
					Iid:      participant[1],
					Positive: true,
				},
			)
		} else {
			result = append(
				result,
				ParticipantAndPhenotype{
					Fid:      participant[0],
					Iid:      participant[1],
					Positive: false,
				},
			)
		}
	}
	return result
}

func WriteSingleToFile(participants []ParticipantAndPhenotype, file_path string) error {
	setUpFolder(filepath.Dir(file_path))

	file_io, err := os.OpenFile(file_path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("Failed to create %v: %v\n", file_path, err)
	}
	defer file_io.Close()

	writer := bufio.NewWriter(file_io)
	defer func() {
		if err := writer.Flush(); err != nil {
			log.Printf("Failed to flush result to %v: %v\n", file_path, err)
		}
	}()

	for _, participant := range participants {
		var phenotype int
		if participant.Positive {
			phenotype = 2
		} else {
			phenotype = 1
		}
		var line string = fmt.Sprintf(
			"%s\t%s\t%d\n",
			participant.Fid,
			participant.Iid,
			phenotype,
		)

		_, err := writer.WriteString(line)
		if err != nil {
			return fmt.Errorf("Failed to write to %v: %v\n", file_path, err)
		}
	}

	return nil
}

// map[phenotype_name][2(FID,IID)]string
//
// return phenotypes' names and []ParticipantAndPhenotypes. The length of `[]string` matches that of ParticipantAndPhenotypes.Phenotypes
func ConvertMultipleToSingle(reference [][2]string, pheno_name_and_positive_participants map[string][][2]string) ([]string, []ParticipantAndPhenotypes) {
	// eliminate rearrangement
	var participants_and_phenotypes = make(
		[]ParticipantAndPhenotypes,
		0,
		len(reference),
	)
	{
		// var reference_temp [][2]string
		var refercence_map = make(map[[2]string]struct{}, 500_000)

		for _, participant := range reference {
			refercence_map[participant] = struct{}{}
		}

		for participant, _ := range refercence_map {
			participants_and_phenotypes = append(
				participants_and_phenotypes,
				ParticipantAndPhenotypes{
					Fid:        participant[0],
					Iid:        participant[1],
					Phenotypes: make([]bool, 0, len(pheno_name_and_positive_participants)),
				},
			)
		}
	}

	var phenotype_names []string
	for phenotype_name, positive_fids_and_iids := range pheno_name_and_positive_participants {
		phenotype_names = append(phenotype_names, phenotype_name)

		// convert `fid_and_iid` to map to increase searching speed
		var positive_fids_and_iids_set = make(map[[2]string]struct{}, 200)
		for _, fid_and_iid := range positive_fids_and_iids {
			positive_fids_and_iids_set[fid_and_iid] = struct{}{}
		}

		// search
		for i := range participants_and_phenotypes {
			fid_and_iid := [2]string{participants_and_phenotypes[i].Fid, participants_and_phenotypes[i].Iid}
			if _, do_contain := positive_fids_and_iids_set[fid_and_iid]; do_contain {
				participants_and_phenotypes[i].Phenotypes = append(participants_and_phenotypes[i].Phenotypes, true)
			} else {
				participants_and_phenotypes[i].Phenotypes = append(participants_and_phenotypes[i].Phenotypes, false)
			}
		}
	}

	return phenotype_names, participants_and_phenotypes
}

func WriteMultipleToFile(file_path string, phenotypes_names []string, participants_and_phenotypes ...ParticipantAndPhenotypes) error {
	// if len(phenotypes_names) != len(participants_and_phenotypes) {
	// 	log.Panicf("Length of phenotypes_names doesn't match that of", v ...any)
	// }
	setUpFolder(filepath.Dir(file_path))

	file_io, err := os.OpenFile(file_path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("Failed to create %v: %v\n", file_path, err)
	}
	defer file_io.Close()

	writer := bufio.NewWriter(file_io)
	defer func() {
		if err := writer.Flush(); err != nil {
			log.Printf("Failed to flush result to %v: %v\n", file_path, err)
		}
	}()

	// Write header line
	{
		header_line := strings.Join(
			append(
				[]string{"FID", "IID"}, phenotypes_names...,
			),
			"\t",
		) + "\n"
		_, err := writer.WriteString(header_line)
		if err != nil {
			return fmt.Errorf("Failed to write to %v: %v\n", file_path, err)
		}
	}

	// Write contents
	for i, participant_and_phenotypes := range participants_and_phenotypes {
		if len(participant_and_phenotypes.Phenotypes) != len(phenotypes_names) {
			return fmt.Errorf(
				"Tokens not matched at line %v: expected %v, got %v",
				i+2,
				len(phenotypes_names),
				len(participant_and_phenotypes.Phenotypes),
			)
		}
		line := strings.Join(
			append(
				[]string{
					participant_and_phenotypes.Fid,
					participant_and_phenotypes.Iid,
				},
				func(bools []bool) string {
					if len(bools) == 0 {
						return ""
					}

					var builder strings.Builder
					builder.Grow(len(bools)*2 - 1)

					for i, b := range bools {
						if i > 0 {
							builder.WriteString("\t")
						}
						if b {
							builder.WriteString("2")
						} else {
							builder.WriteString("1")
						}
					}

					return builder.String()
				}(participant_and_phenotypes.Phenotypes),
			),
			"\t",
		) + "\n"
		_, err := writer.WriteString(line)
		if err != nil {
			return fmt.Errorf("Failed to write to %v: %v", file_path, err)
		}
	}
	return nil
}
