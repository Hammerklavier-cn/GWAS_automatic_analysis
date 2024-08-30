plink_path="plink"
file_name="../testsuite/STAB2_sample2.vcf.gz"
phenotype="mydata3.csv"
ethnic="../testsuite/Ethnic background.xlsx"
ethnic_ref="../myutil/ethnic_serial_reference.tsv"

cd "$(dirname "$0")"    # 文件所在目录
mkdir -p ../test/
cd ../test/                  # 项目主目录
python3 ../main.py \
    --single \
    --plink-path "${plink_path}" \
    --file-name "${file_name}" \
    --phenotype "${phenotype}" \
    --ethnic "${ethnic}" \
    --ethnic-reference "${ethnic_ref}" 

# after test finished
# rm -r ./temp
# rm *