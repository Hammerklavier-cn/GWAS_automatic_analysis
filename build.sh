WORKING_DIR=$(pwd)
cd $(dirname "$0")
SCRIPT_DIR=$(pwd)

# Allow external variables override
CC=${CC:-gcc}
CXX=${CXX:-g++}
OUTPUT_DIR=${OUTPUT:-"$WORKING_DIR/compile"}

if ! command -v nuitka &> /dev/null; then
    echo "Nuitka is not installed. It will be installed automatically."
    pip install nuitka
fi

if ! command -v go &> /dev/null; then
    echo "Go is not installed."
    exit 1
fi
echo "go version $(go version)"

if ! command -v cargo &> /dev/null; then
    echo "Rust not found."
    exit 1
fi
echo "rustc version $(rustc --version)"

if ! command -v python &> /dev/null; then
    echo "Python not found."
    exit 1
elif ! python --version 2>&1 | grep -qE '^Python 3\.[1-9][0-9]\.'; then
    echo "Python version must be higher than 3.10. Current version is $(python --version)"
    exit 1
fi
echo "python version $(python --version)"

if [ -e "$OUTPUT_DIR" ] || [ -d "$OUTPUT_DIR" ]; then
    rm -rf "$OUTPUT_DIR"
fi

mkdir -p $OUTPUT_DIR
if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Failed to create output directory"
    exit 1
fi


# build main.py
cd $SCRIPT_DIR
rm -rf dist
if [ "$CC" == "clang" ]; then
    nuitka main.py --onefile --clang
    exitcode_main=$?
elif [ "$CC" == "cl.exe" ]; then
    nuitka main.py --onefile --msvc=latest
    exitcode_main=$?
else
    nuitka main.py --onefile
    exitcode_main=$?
fi

if [ $exitcode_main -ne 0 ]; then
    echo "Failed to build main.py"
    exitcode_main=1
else
    # copy result to output directory
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "mingw32" ]]; then
        cp "$SCRIPT_DIR/main.exe" "$OUTPUT_DIR/GWAS_automatic_analysis.exe"
    else
        cp "$SCRIPT_DIR/main.bin" "$OUTPUT_DIR/GWAS_automatic_analysis"
    fi
    # clean up dist directory
    rm -rf main.{bin,build,dist,onefile-build}
    echo "Successfully built main.py"
fi


# build toolkit/annotate_positive_snps
cd $SCRIPT_DIR
cd toolkit/annotate_positive_snps
cargo clean
cargo build --release
exitcode_annotate_positive_snps=$?
if [ $exitcode_annotate_positive_snps -ne 0 ]; then
    echo "Failed to build annotate_positive_snps"
    exitcode_annotate_positive_snps=1
else
    echo "Successfully built annotate_positive_snps"
    # move
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "mingw32" ]]; then
        cp "$SCRIPT_DIR/toolkit/annotate_positive_snps/target/release/annotate_positive_snps.exe" "$OUTPUT_DIR"
    else
        cp "$SCRIPT_DIR/toolkit/annotate_positive_snps/target/release/annotate_positive_snps" "$OUTPUT_DIR"
    fi
    # clean
    cargo clean
fi


# build toolkit/binary_phenotype_genesis
cd $SCRIPT_DIR/toolkit/binary_phenotype_genesis
go clean -cache

if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "mingw32" ]]; then
    go build -a -buildmode=pie -o "$OUTPUT_DIR/binary_phenotype_genesis.exe" main.go
else
    go build -a -buildmode=pie -o "$OUTPUT_DIR/binary_phenotype_genesis" main.go
fi
exitcode_binary_phenotype_genesis=$?
if [ $exitcode_binary_phenotype_genesis -ne 0 ]; then
    echo "Failed to build binary_phenotype_genesis"
    exitcode_binary_phenotype_genesis=1
else
    go clean -cache
    echo "Successfully built binary_phenotype_genesis"
fi


# build toolkit/extract_csv_columns
cd $SCRIPT_DIR/toolkit/extract_csv_columns
rm -rf $SCRIPT_DIR/toolkit/extract_csv_columns/extract_csv_columns.{bin,build,dist,onefile-build}
if [ "$CC" == "clang" ]; then
    nuitka extract_csv_columns.py --onefile --clang
    exitcode_extract_csv_columns=$?
elif [ "$CC" == "cl.exe" ]; then
    nuitka extract_csv_columns.py --onefile --msvc=latest
    exitcode_extract_csv_columns=$?
else
    nuitka extract_csv_columns.py --onefile
    exitcode_extract_csv_columns=$?
fi

if [ $exitcode_extract_csv_columns -ne 0 ]; then
    echo "Failed to build extract_csv_columns"
    exitcode_extract_csv_columns=1
else
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "mingw32" ]]; then
        cp "$SCRIPT_DIR/toolkit/extract_csv_columns/extract_csv_columns.exe" "$OUTPUT_DIR"
    else
        # For Linux/Unix systems, Nuitka may produce different file names
        cp "$SCRIPT_DIR/toolkit/extract_csv_columns/extract_csv_columns.bin" "$OUTPUT_DIR/extract_csv_columns"
        if [ $? -ne 0 ]; then
            echo "Failed to copy extract_csv_columns"
            exitcode_extract_csv_columns=1
        fi
    fi
    echo "Successfully built extract_csv_columns"
fi
rm -rf $SCRIPT_DIR/toolkit/extract_csv_columns/extract_csv_columns.{bin,build,dist,onefile-build}
if [ $? -ne 0 ]; then
    echo "Failed to clean up extract_csv_columns"
    exitcode_extract_csv_columns=1
fi


# build result_analysis
cd $SCRIPT_DIR/toolkit/result_analysis
rm -rf $SCRIPT_DIR/toolkit/result_analysis/main.{bin,build,dist,onefile-build}
if [ "$CC" == "clang" ]; then
    nuitka main.py --onefile --clang
    exitcode_result_analysis=$?
elif [ "$CC" == "cl.exe" ]; then
    nuitka main.py --onefile --msvc=latest
    exitcode_result_analysis=$?
else
    nuitka main.py --onefile
    exitcode_result_analysis=$?
fi

if [ $exitcode_result_analysis -ne 0 ]; then
    echo "Failed to build result_analysis"
    exitcode_result_analysis=1
else
    if [ "$OSTYPE" == "msys" ] || [ "$OSTYPE" == "win32" ] || [ "$OSTYPE" == "mingw32" ]; then
        cp "$SCRIPT_DIR/toolkit/result_analysis/main.exe" "$OUTPUT_DIR/result_analysis.exe"
    else
        cp "$SCRIPT_DIR/toolkit/result_analysis/main.bin" "$OUTPUT_DIR/result_analysis"
    fi

    if [ $? -eq 0 ]; then
        echo "Successfully built result_analysis"
    else
        echo "Failed to copy result_analysis"
        exitcode_result_analysis=1
    fi
fi
rm -rf $SCRIPT_DIR/toolkit/result_analysis/main.{bin,build,dist,onefile-build}
if [ $? -ne 0 ]; then
    echo "Failed to clean up result_analysis"
    exitcode_result_analysis=1
fi


echo ""
echo "----------------------------"
# Count failures properly - if exit code is not 0, count as failure
failures=0
[ $exitcode_main -eq 0 ] || failures=$((failures + 1))
[ $exitcode_annotate_positive_snps -eq 0 ] || failures=$((failures + 1))
[ $exitcode_binary_phenotype_genesis -eq 0 ] || failures=$((failures + 1))
[ $exitcode_extract_csv_columns -eq 0 ] || failures=$((failures + 1))
[ $exitcode_result_analysis -eq 0 ] || failures=$((failures + 1))

echo "5 tasks in total, $failures failed."

# final check
echo "files locate in $OUTPUT_DIR:"
ls $OUTPUT_DIR/GWAS_automatic_analysis
ls $OUTPUT_DIR/annotate_positive_snps
ls $OUTPUT_DIR/binary_phenotype_genesis
ls $OUTPUT_DIR/extract_csv_columns
ls $OUTPUT_DIR/result_analysis
