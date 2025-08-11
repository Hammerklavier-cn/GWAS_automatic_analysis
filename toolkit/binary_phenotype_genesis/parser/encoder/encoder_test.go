package encoder

import (
	"os"
	"reflect"
	"slices"
	"testing"
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

var phenotype_2_participants = [][2]string{
	{"participant01", "participant01"},
	{"participant03", "participant03"},
	{"participant_infinite", "participant_infinite"},
	{"participant_quoted", "participant_quoted"},
}

var phenotype_3_participants = [][2]string{
	{"Family 1", "Person A"},
	{"Family 3", "Person A"},
	{"Family 3", "Person B"},
	{"Family_5", "Person_Z"},
	{"participant01", "participant01"},
	{"participant03", "participant03"},
	{"participant_infinite", "participant_infinite"},
	{"participant_quoted", "participant_quoted"},
}

var ref_result = []ParticipantAndPhenotype{
	{Fid: "Family 1", Iid: "Person A", Positive: true},
	{Fid: "Family 1", Iid: "Person B", Positive: false},
	{Fid: "Family 2", Iid: "Person A", Positive: false},
	{Fid: "Family 3", Iid: "Person A", Positive: true},
	{Fid: "Family 3", Iid: "Person B", Positive: true},
	{Fid: "Family 3", Iid: "Person C", Positive: false},
	{Fid: "Family 3", Iid: "Person D", Positive: false},
	{Fid: "Family 3", Iid: "Person E", Positive: false},
	{Fid: "Family 4", Iid: "Person X", Positive: false},
	{Fid: "Family_5", Iid: "Person_Z", Positive: true},
	{Fid: "participant01", Iid: "participant01", Positive: true},
	{Fid: "participant02", Iid: "participant02", Positive: false},
	{Fid: "participant03", Iid: "participant03", Positive: true},
	{Fid: "participant04", Iid: "participant04", Positive: false},
	{Fid: "participant_infinite", Iid: "participant_infinite", Positive: true},
	{Fid: "null", Iid: "null", Positive: false},
	{Fid: "nan", Iid: "nan", Positive: false},
	{Fid: "participant_quoted", Iid: "participant_quoted", Positive: true},
}

var ref_multiple_result = []ParticipantAndPhenotypes{
	{Fid: "Family 1", Iid: "Person A", Phenotypes: []bool{false, true}},
	{Fid: "Family 1", Iid: "Person B", Phenotypes: []bool{false, false}},
	{Fid: "Family 2", Iid: "Person A", Phenotypes: []bool{false, false}},
	{Fid: "Family 3", Iid: "Person A", Phenotypes: []bool{false, true}},
	{Fid: "Family 3", Iid: "Person B", Phenotypes: []bool{false, true}},
	{Fid: "Family 3", Iid: "Person C", Phenotypes: []bool{false, false}},
	{Fid: "Family 3", Iid: "Person D", Phenotypes: []bool{false, false}},
	{Fid: "Family 3", Iid: "Person E", Phenotypes: []bool{false, false}},
	{Fid: "Family 4", Iid: "Person X", Phenotypes: []bool{false, false}},
	{Fid: "Family_5", Iid: "Person_Z", Phenotypes: []bool{false, true}},
	{Fid: "participant01", Iid: "participant01", Phenotypes: []bool{true, true}},
	{Fid: "participant02", Iid: "participant02", Phenotypes: []bool{false, false}},
	{Fid: "participant03", Iid: "participant03", Phenotypes: []bool{true, true}},
	{Fid: "participant04", Iid: "participant04", Phenotypes: []bool{false, false}},
	{Fid: "participant_infinite", Iid: "participant_infinite", Phenotypes: []bool{true, true}},
	{Fid: "null", Iid: "null", Phenotypes: []bool{false, false}},
	{Fid: "nan", Iid: "nan", Phenotypes: []bool{false, false}},
	{Fid: "participant_quoted", Iid: "participant_quoted", Phenotypes: []bool{true, true}},
}

func assertEqual(t *testing.T, expected, actual any) {
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v, got %v\n", expected, actual)
	}
}

func TestGenerateSingle(t *testing.T) {
	// test 1
	{
		res := GenerateSingle(
			fid_and_iid_reference,
			phenotype_3_participants,
		)
		// if !reflect.DeepEqual(res, ref_result) {
		// 	t.Errorf("Expected %v, got %v", ref_result, res)
		// }
		assertEqual(t, ref_result, res)
	}
}

func TestWriteSingleToFile(t *testing.T) {
	{
		file_path := "test_result.tsv"
		err := WriteSingleToFile(
			ref_result,
			file_path,
		)
		defer os.Remove(file_path)
		if err != nil {
			t.Errorf("Error writing to file: %v", err)
		}
	}
}

func TestConvertMultipleToSingle(t *testing.T) {
	pheno_name_and_participants_map := make(map[string][][2]string, 2)
	pheno_name_and_participants_map["phenotype_2"] = phenotype_2_participants
	pheno_name_and_participants_map["phenotype_3"] = phenotype_3_participants

	phenotypes_order, res := ConvertMultipleToSingle(
		fid_and_iid_reference,
		pheno_name_and_participants_map,
	)

	for _, participant_and_phenotypes := range res {
		for i, phenotype_name := range phenotypes_order {
			var participant_is_positive bool = participant_and_phenotypes.Phenotypes[i]

			ref_positive_participants := pheno_name_and_participants_map[phenotype_name]
			var ref_participant_is_positive bool = slices.Contains(
				ref_positive_participants,
				[2]string{participant_and_phenotypes.Fid, participant_and_phenotypes.Iid},
			)

			assertEqual(t, ref_participant_is_positive, participant_is_positive)
		}
	}

	// Rewrite
	for i, phenotype_name := range phenotypes_order {

		ref_positive_participants := pheno_name_and_participants_map[phenotype_name]
		// Convert to map for efficient lookup
		ref_positive_participants_map := make(map[[2]string]struct{}, len(ref_positive_participants))
		for _, participant := range ref_positive_participants {
			ref_positive_participants_map[participant] = struct{}{}
		}

		// Iterate over participants and check if they are positive
		for _, participant_and_phenotypes := range res {
			var participant_is_positive bool = participant_and_phenotypes.Phenotypes[i]

			var _, ref_participant_is_positive = ref_positive_participants_map[[2]string{
				participant_and_phenotypes.Fid,
				participant_and_phenotypes.Iid,
			}]

			assertEqual(t, ref_participant_is_positive, participant_is_positive)
		}
	}
}

func TestWriteMultipleToFile(t *testing.T) {
	var file_path = "test_multiple_results.tsv"

	err := WriteMultipleToFile(
		file_path,
		[]string{"phenotype_2", "phenotype_3"},
		ref_multiple_result...,
	)
	if err != nil {
		t.Errorf("WriteMultipleToFile failed: %v\n", err)
	}
	defer os.Remove(file_path)
}
