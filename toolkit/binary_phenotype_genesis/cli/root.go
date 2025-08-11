package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	// Inputs
	InputFiles          []string
	InputFolder         string
	AllParticipantsFile string

	// Input options
	InputFilesHaveNoHeader bool

	// Outputs
	OutputPrefix string

	// Output options
	OutputSingleFile    bool
	OutputMultipleFiles bool

	// Format options
	NoReplaceSpaceByUnderline bool

	rootCmd = &cobra.Command{
		Use:   "binary_phenotype_genesis",
		Short: "Generate a binary phenotype file from IID or PID+IID",
		Long: "Generate one or multiple binary phenotype file(s), containing multiple / one " +
			"phenotype value, from merely participants' IID (FID will be completed as IID) " +
			"or FID + IID.",

		Version: "0.1.0",

		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Run exec")
		},
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// cobra.OnInitialize(y ...func())

	// Inputs
	rootCmd.PersistentFlags().StringSliceVarP(
		&InputFiles,
		"input-files", "i", []string{},
		"file containing positive participants' ID",
	)
	rootCmd.PersistentFlags().StringVarP(
		&InputFolder,
		"input-folder", "f", "",
		"folder containing files of positive participants' ID",
	)
	rootCmd.PersistentFlags().StringVarP(
		&AllParticipantsFile,
		"all-participants", "a", "",
		"File containing all Participants' ID",
	)

	rootCmd.MarkFlagsOneRequired(
		"input-files", "input-folder",
	)
	rootCmd.MarkPersistentFlagRequired("all-participants")
	rootCmd.MarkPersistentFlagDirname("input-folder")

	// Input options
	rootCmd.PersistentFlags().BoolVar(
		&InputFilesHaveNoHeader,
		"no-header-line", false,
		"Set if input files do not have header line",
	)

	// Output
	rootCmd.PersistentFlags().StringVarP(
		&OutputPrefix, "out", "o", "",
		"Prefix of the output file(s)",
	)

	// Output options
	rootCmd.PersistentFlags().BoolVar(
		&OutputSingleFile,
		"make-single", false,
		"Generate a single file containing all the phenotypes [Default option]",
	)
	rootCmd.PersistentFlags().BoolVar(
		&OutputMultipleFiles,
		"make-separate", false,
		"Generate multiple files, each containing single phenotype",
	)
	rootCmd.MarkFlagsMutuallyExclusive("make-single", "make-separate")

	// Format options
	rootCmd.PersistentFlags().BoolVar(
		&NoReplaceSpaceByUnderline,
		"no-replace-space-with-underline", false,
		"Set to disable replacing spaces with underlines in phenotype names",
	)

	// Exit on help
	// help_func := rootCmd.HelpFunc()
	// rootCmd.SetHelpFunc(func(c *cobra.Command, s []string) {
	// 	help_func(c, s)
	// 	os.Exit(0)
	// })

}
