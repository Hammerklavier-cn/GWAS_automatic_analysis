python3 main.py \
    -i /home/jimmy/projects/GWAS_automatic_analysis/results/MRC1-clean_ethnic-results/MRC1-part1-split_gender/summary-q_adjusted.tsv \
        /home/jimmy/projects/GWAS_automatic_analysis/results/MRC1-clean_ethnic-results/MRC1-part2-split_gender/summary-q_adjusted.tsv \
        /home/jimmy/projects/GWAS_automatic_analysis/results/MRC1-clean_ethnic-results/MRC1-part3-split_gender/summary-q_adjusted.tsv \
    --reference run/reference.tsv \
    -a all \
    -o results_clean-ethnic
