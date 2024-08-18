plink_path="plink"
file_name="../testsuit/STAB2.vcf.gz"
phenotype="mydata3.csv"

cd "$(dirname "$0")"    # 文件所在目录
mkdir -p ../test/
cd ../test/                  # 项目主目录
python3 ../main.py \
    --single \
    --plink-path "${plink_path}" \
    --file-name "${file_name}" \
    --phenotype "${phenotype}" 

# after test finished
rm -r ./temp
rm *