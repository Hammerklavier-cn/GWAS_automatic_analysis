package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

var SCRIPT_DIR string = func() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatalln("Failed to get current file path")
	}
	root_dir := filepath.Dir(filename)
	log.Printf("The script locates in %s\n", root_dir)
	return root_dir
}()

var WORKING_DIR string = func() string {
	working_dir, err := os.Getwd()
	if err != nil {
		log.Fatalln("Failed to get working directory")
	}
	log.Printf("Working directory: %s\n", working_dir)
	return working_dir
}()

var date string = time.Now().Format("20060102")

type ProjectName string

const (
	MAIN_PROJECT             ProjectName = "main"
	ANNOTATE_POSITIVE_SNPS   ProjectName = "annotate_positive_snps"
	BINARY_PHENOTYPE_GENESIS ProjectName = "binary_phenotype_genesis"
	EXTRACT_CSV_COLUMNS      ProjectName = "extract_csv_columns"
	RESULT_ANALYSIS          ProjectName = "result_analysis"
)

var targets = []ProjectName{
	MAIN_PROJECT,
	ANNOTATE_POSITIVE_SNPS,
	BINARY_PHENOTYPE_GENESIS,
	EXTRACT_CSV_COLUMNS,
	RESULT_ANALYSIS,
}

var script_version string = "0.1.0"

const help string = `GWAS Automatic Analysis Build Tool

Usage:
  build [flags] [projects...]

Flags:
  -h, --help      Show this help message
  -v, --version   Show version information

Projects:
  main                     Main GWAS analysis pipeline
  annotate_positive_snps   Annotate significant SNPs
  binary_phenotype_genesis Generate binary phenotypes
  extract_csv_columns      Extract columns from CSV files
  result_analysis          Analyze GWAS results
  all                      Build all projects

Examples:
  build main
  build annotate_positive_snps result_analysis
  build all`

func checkPythonEnv() error {
	var python3_not_found bool
	var python_not_found bool
	if _, err := exec.LookPath("python3"); err != nil {
		python3_not_found = true
	}
	if _, err := exec.LookPath("python"); err != nil {
		python_not_found = true
	}
	if python3_not_found && python_not_found {
		return fmt.Errorf("python3 not found")
	}

	cmd := exec.Command("python3", "--version")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to check python version: %v", err)
	}

	versionStr := strings.TrimSpace(string(output))
	versionParts := strings.Split(versionStr, " ")
	if len(versionParts) < 2 {
		return fmt.Errorf("invalid python version string: %s", versionStr)
	}

	version := versionParts[1]
	versionComponents := strings.Split(version, ".")
	if len(versionComponents) < 2 {
		return fmt.Errorf("invalid python version format: %s", version)
	}

	major, err := strconv.Atoi(versionComponents[0])
	if err != nil {
		return fmt.Errorf("invalid major version: %s", versionComponents[0])
	}

	minor, err := strconv.Atoi(versionComponents[1])
	if err != nil {
		return fmt.Errorf("invalid minor version: %s", versionComponents[1])
	}

	if major < 3 || (major == 3 && minor < 10) {
		return fmt.Errorf("python version must be >= 3.10, found %s", version)
	}

	return nil
}

func checkGoEnv() error {
	if _, err := exec.LookPath("go"); err != nil {
		return fmt.Errorf("go not found")
	}
	return nil
}

func checkRustEnv() error {
	if _, err := exec.LookPath("rustc"); err != nil {
		return fmt.Errorf("rust not found")
	}
	return nil
}

type Project interface {
	// Build
	Build() error

	// Copy the compilation result to target path.
	CopyTo(folder string) error

	// Remove the compilation result and temp files
	Clean()
}

type pythonProject struct {
	// name of the project
	name string
	// name of the entrance file, like `main.py`
	entrance string
	//
	script_dir string
	// output path
	out_path string
}

// name (string): the name of the project
// entrance (string): name of the entrance file, e.g. `main.py`
func NewPythonProject(name, entrance, script_dir string) *pythonProject {
	return &pythonProject{
		name:       name,
		entrance:   entrance,
		script_dir: script_dir,
	}
}

func (p *pythonProject) Build() error {
	os.Chdir(p.script_dir)
	defer os.Chdir(WORKING_DIR)

	entrance_path := filepath.Join(p.script_dir, p.entrance)
	if fileInfo, err := os.Stat(entrance_path); os.IsNotExist(err) {
		return err
	} else if fileInfo.IsDir() {
		return fmt.Errorf("%s is a directory, not file\n", entrance_path)
	} else if filepath.Ext(fileInfo.Name()) != ".py" {
		return fmt.Errorf("%s is not a Python script!\n", entrance_path)
	}

	p.Clean()

	cmd := exec.Command(
		"nuitka",
		entrance_path,
		"--onefile",
	)

	pipe_err, err := cmd.StderrPipe()
	if err != nil {
		log.Println("Failed to create stderr pipe.")
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Failed to start nuitka: %v\n", err)
	}

	scanner := bufio.NewScanner(pipe_err)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			log.Println("Nuitka stderr:", line)
		}
		if err := scanner.Err(); err != nil {
			log.Println("Error reading stderr:", err)
		}
	}()

	err = cmd.Wait()

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("Nuitka failed with exit code %d\n", exitErr.ExitCode())
		} else {
			return fmt.Errorf("Error waiting for Nuitka: %v\n", err)
		}
	}

	return nil
}

func (p *pythonProject) Clean() {

	current_dir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Failed to get current directory: %s\n", err)
		return
	}

	os.Chdir(p.script_dir)
	defer os.Chdir(current_dir)

	paths := []string{
		filepath.Join(p.script_dir, strings.TrimSuffix(p.entrance, ".py")+".build"),
		filepath.Join(p.script_dir, strings.TrimSuffix(p.entrance, ".py")+".dist"),
		filepath.Join(p.script_dir, strings.TrimSuffix(p.entrance, ".py")+".onefile-build"),
	}

	switch runtime.GOOS {
	case "windows":
		paths = append(paths, filepath.Join(p.script_dir, strings.TrimSuffix(p.entrance, ".py")+".exe"))
	default:
		paths = append(paths, filepath.Join(p.script_dir, strings.TrimSuffix(p.entrance, ".py")+".bin"))
	}

	for _, path := range paths {
		if fileInfo, err := os.Stat(path); os.IsNotExist(err) {
			log.Printf("%v not found. Skip removing it\n", path)
		} else if err != nil {
			log.Println("Unknown error:", err)
		} else if fileInfo.IsDir() {
			os.RemoveAll(path)
		} else {
			os.Remove(path)
		}
	}
}

func (p *pythonProject) CopyTo(folder string) error {
	// Determine source and target path
	var source_path string = filepath.Join(p.script_dir, strings.TrimSuffix(p.entrance, ".py"))
	var target_path string = fmt.Sprintf("%v_%v", filepath.Join(folder, p.name), date)

	switch runtime.GOOS {
	case "windows":
		source_path += ".exe"
	case "linux":
		source_path += ".bin"
	case "darwin":
		source_path += ".bin"
	}

	target_path = fmt.Sprintf("%s_%s_%s", target_path, runtime.GOOS, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		target_path += ".exe"
	}

	p.out_path = target_path

	if err := os.MkdirAll(filepath.Dir(target_path), 0755); err != nil {
		return err
	}

	// Source file
	source_file, err := os.Open(source_path)
	if err != nil {
		return err
	}
	defer source_file.Close()

	// Target file
	target_file, err := os.Create(target_path)
	if err != nil {
		return fmt.Errorf("Failed to create %s: %v\n", target_path, err)
	}
	defer target_file.Close()

	// Copy
	_, err = io.Copy(target_file, source_file)
	if err != nil {
		return fmt.Errorf("Failed to copy %s to %s: %s\n", source_path, target_path, err)
	}

	return nil
}

type rustProject struct {
	// name of the project/subproject
	name string
	// root folder of the project
	script_dir string
	// output path, where compiled result will be exported to.
	out_path string
}

func NewRustProject(name, script_dir string) *rustProject {
	return &rustProject{
		name:       name,
		script_dir: script_dir,
	}
}

func (r *rustProject) Build() error {
	os.Chdir(r.script_dir)
	defer os.Chdir(WORKING_DIR)

	r.Clean()

	cmd := exec.Command("cargo", "build", "--release")

	pipe_err, err := cmd.StderrPipe()
	if err != nil {
		log.Println("Failed to create stderr pipe.")
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Failed to execute cargo build: %v\n", err)
	}

	scanner := bufio.NewScanner(pipe_err)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			log.Println("cargo stderr:", line)
		}
		if err := scanner.Err(); err != nil {
			log.Println("Error reading stderr:", err)
		}
	}()

	err = cmd.Wait()

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("Cargo build failed with exit code %d\n", exitErr.ExitCode())
		} else {
			return fmt.Errorf("Error executing cargo build: %v\n", err)
		}
	}

	return nil
}

func (r *rustProject) Clean() {
	current_dir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Failed to get current directory: %s\n", err)
		return
	}

	os.Chdir(r.script_dir)
	defer os.Chdir(current_dir)

	cmd := exec.Command("cargo", "clean")

	cmd.Run()
}

func (r *rustProject) CopyTo(folder string) error {
	var source_path string = filepath.Join(r.script_dir, "target", "release", r.name)
	var target_path string = fmt.Sprintf("%s_%s", filepath.Join(folder, r.name), date)

	switch runtime.GOOS {
	case "windows":
		source_path += ".exe"
	}

	target_path = fmt.Sprintf("%s_%s_%s", target_path, runtime.GOOS, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		target_path += ".exe"
	}

	r.out_path = target_path

	if err := os.MkdirAll(filepath.Dir(target_path), 0755); err != nil {
		return err
	}

	// Open source file
	source_file, err := os.Open(source_path)
	if err != nil {
		return fmt.Errorf("failed to open %s: %s\n", source_path, err)
	}
	defer source_file.Close()

	// Create target file
	target_file, err := os.Create(target_path)
	if err != nil {
		return fmt.Errorf("Failed to create %s: %s\n", target_path, err)
	}
	defer target_file.Close()

	// copy
	_, err = io.Copy(target_file, source_file)
	if err != nil {
		return fmt.Errorf("Failed to copy %s to %s: %s\n", source_path, target_path, err)
	}

	return nil
}

type goProject struct {
	name       string
	entrance   string
	script_dir string
	out_path   string
}

func NewGoProject(name, entrance, script_dir string) *goProject {
	return &goProject{
		name:       name,
		entrance:   entrance,
		script_dir: script_dir,
	}
}

func (g *goProject) Build() error {
	os.Chdir(g.script_dir)
	defer os.Chdir(WORKING_DIR)

	entrance_path := filepath.Join(g.script_dir, g.entrance)
	if fileInfo, err := os.Stat(entrance_path); os.IsNotExist(err) {
		return err
	} else if fileInfo.IsDir() {
		return fmt.Errorf("%s is a directory, not file\n", entrance_path)
	} else if filepath.Ext(fileInfo.Name()) != ".go" {
		return fmt.Errorf("%s is not a go script!\n", entrance_path)
	}

	g.Clean()

	// Build
	cmd := exec.Command("go", "build", entrance_path)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Failed to start go %s: %s\n", g.name, err)
	}

	pipe_err, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("Failed to get stderr pipe for %s: %s\n", g.name, err)
	}

	go func() {
		scanner := bufio.NewScanner(pipe_err)
		for scanner.Scan() {
			fmt.Printf("stderr: %s\n", scanner.Text())
		}
	}()

	err = cmd.Wait()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			fmt.Printf("Failed to build %s with exit code %d: %s\n", g.name, exitErr.ExitCode(), exitErr)
		} else {
			fmt.Printf("Failed to build %s: %s\n", g.name, err)
		}
	}

	return nil
}

func (g *goProject) Clean() {
	current_dir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Failed to get current directory: %s\n", err)
		return
	}

	os.Chdir(g.script_dir)
	defer os.Chdir(current_dir)

	var path string = filepath.Join(g.script_dir, strings.TrimSuffix(g.entrance, ".go"))

	if runtime.GOOS == "windows" {
		path += ".exe"
	}

	if _, err := os.Stat(path); err == nil {
		os.Remove(path)
	}

	cmd := exec.Command("go", "clean", "-cache")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Printf("Failed to clean %s: %s\n", g.name, err)
	}
	time.Sleep(2 * time.Second)
}

func (g *goProject) CopyTo(folder string) error {
	var source_path string = filepath.Join(g.script_dir, strings.TrimSuffix(g.entrance, ".go"))
	var target_path string = fmt.Sprintf("%s_%s", filepath.Join(folder, g.name), date)

	if runtime.GOOS == "windows" {
		source_path += ".exe"
	}

	target_path = fmt.Sprintf("%s_%s_%s", target_path, runtime.GOOS, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		target_path += ".exe"
	}

	g.out_path = target_path

	if err := os.MkdirAll(filepath.Dir(target_path), 0755); err != nil {
		return err
	}

	// Open source file
	if _, err := os.Stat(source_path); err != nil {
		panic(err)
	}
	source_file, err := os.Open(source_path)
	if err != nil {
		return err
	}
	defer source_file.Close()

	// Create target file
	target_file, err := os.Create(target_path)
	if err != nil {
		return err
	}
	defer target_file.Close()

	// Copy source file to target file
	_, err = io.Copy(target_file, source_file)
	if err != nil {
		return err
	}

	return nil
}

// assert that the executable locates in the root folder of the project.
func main() {
	if slices.Contains(os.Args[1:], "--help") || slices.Contains(os.Args[1:], "-h") {
		fmt.Println(help)
		os.Exit(0)
	} else if slices.Contains(os.Args[1:], "--version") || slices.Contains(os.Args[1:], "-v") {
		fmt.Println("GWAS_automatic_analysis build script version", script_version)
		os.Exit(0)
	} else if len(os.Args) < 2 {
		fmt.Println(help)
		os.Exit(1)
	}

	var main_project *pythonProject = NewPythonProject(
		"GWAS_automatic_analysis",
		"main.py",
		SCRIPT_DIR,
	)
	var annotate_positive_snps_project *rustProject = NewRustProject(
		string(ANNOTATE_POSITIVE_SNPS),
		filepath.Join(SCRIPT_DIR, "toolkit", string(ANNOTATE_POSITIVE_SNPS)),
	)
	var binary_phenotype_genesis_project *goProject = NewGoProject(
		string(BINARY_PHENOTYPE_GENESIS),
		"main.go",
		filepath.Join(SCRIPT_DIR, "toolkit", string(BINARY_PHENOTYPE_GENESIS)),
	)
	var extract_csv_columns_project *pythonProject = NewPythonProject(
		string(EXTRACT_CSV_COLUMNS),
		"extract_csv_columns.py",
		filepath.Join(SCRIPT_DIR, "toolkit", string(EXTRACT_CSV_COLUMNS)),
	)
	var result_analysis_project *pythonProject = NewPythonProject(
		string(RESULT_ANALYSIS),
		"main.py",
		filepath.Join(SCRIPT_DIR, "toolkit", string(RESULT_ANALYSIS)),
	)

	var check_python_env bool
	var check_rust_env bool
	var check_go_env bool

	var projects_map = make(map[Project]bool, 5)
	projects_map[main_project] = false
	projects_map[annotate_positive_snps_project] = false
	projects_map[binary_phenotype_genesis_project] = false
	projects_map[extract_csv_columns_project] = false
	projects_map[result_analysis_project] = false

	if slices.Contains(os.Args, "all") {
		log.Println("Building all projects")

		check_python_env = true
		check_rust_env = true
		check_go_env = true

		for project := range projects_map {
			projects_map[project] = true
		}

		goto check_and_run
	}

	for _, arg := range os.Args[1:] {
		if !slices.Contains(targets, ProjectName(strings.ToLower(arg))) {
			log.Fatalf("%s is not an valid argument", arg)
			fmt.Println(help)
		}

		switch ProjectName(strings.ToLower(arg)) {
		case MAIN_PROJECT:
			projects_map[main_project] = true
			check_python_env = true
		case ANNOTATE_POSITIVE_SNPS:
			projects_map[annotate_positive_snps_project] = true
			check_rust_env = true
		case BINARY_PHENOTYPE_GENESIS:
			projects_map[binary_phenotype_genesis_project] = true
			check_go_env = true
		case EXTRACT_CSV_COLUMNS:
			projects_map[extract_csv_columns_project] = true
			check_python_env = true
		case RESULT_ANALYSIS:
			projects_map[result_analysis_project] = true
			check_python_env = true
		}
	}

check_and_run:
	if check_python_env {
		if err := checkPythonEnv(); err != nil {
			log.Fatalf("Python environment check failed: %v\n", err)
		}
		log.Println("Python environment check passed")
	}
	if check_rust_env {
		if err := checkRustEnv(); err != nil {
			log.Fatalf("Rust environment check failed: %v\n", err)
		}
		log.Println("Rust environment check passed")
	}
	if check_go_env {
		if err := checkGoEnv(); err != nil {
			log.Fatalf("Go environment check failed: %v\n", err)
		}
		log.Println("Go environment check passed")
	}

	for project, ok := range projects_map {
		if ok {
			log.Printf("Building %s", project)

			// Build the project
			if err := project.Build(); err != nil {
				log.Printf("Failed to build %s: %v\n", project, err)
			}
			log.Printf("%s built successfully", project)

			// Copy the built binary to the output directory
			time.Sleep(5 * time.Second)
			if err := project.CopyTo(filepath.Join(SCRIPT_DIR, "compiled")); err != nil {
				log.Printf("Failed to copy %s: %v\n", project, err)
			}

			log.Printf("%s copied successfully\n", project)

			project.Clean()
		}
	}
}
