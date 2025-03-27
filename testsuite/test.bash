plink_path="plink"
file_name="../testsuite/STAB2.bed"
ethnic="../testsuite/Ethnic background.xlsx"
ethnic_ref="../myutil/ethnic_serial_reference.tsv"
phenotype="../testsuite/blood_chemistry.csv"
gender="../testsuite/Sex.xlsx"
gender_ref="../myutil/gender_serial_reference.csv"

clear

cd "$(dirname "$0")"    # 文件所在目录
mkdir -p ../test/
rm -r ../test/
mkdir -p ../test/
cd ../test/                  # 项目主目录
rm -r *

python3 ../main.py \
    --single \
    --plink-path "${plink_path}" \
    --file-name "${file_name}" \
    --phenotype "${phenotype}" \
    --ethnic "${ethnic}" \
    --loose-ethnic-filter \
    --ethnic-reference "${ethnic_ref}" \
    --gender "${gender}" \
    --gender-reference "${gender_ref}"

# after test finished
# rm -r ./temp
# rm *
