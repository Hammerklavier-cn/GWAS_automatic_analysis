# Subproject: Binary Phenotype Genesis

## Introduction

Accepting a list of all participants and a list of positive ones, this projects
generates a file describing participants' phenotype.

## Input files

Files should either containing only IID, in which case FID will be in coordination
with IID, or providing both FID and IID, IID following FID, with comma(,) as
separator in a csv file or otherwise tab(\t) as separator. Each line should contain
one individual.

## Output file(s)

### Contents

The output file consists of three columns, with each line describing one participant:

- _FID_: FID of individuals. Copied from IID if not provided.
- _IID_: IID of individuals.
- _PHENOTYPE_NAME_1_: PHENOTYPE of the individuals. Underlines(\_) will replace all
  spaces unless `--no-replace-space-with-underline` is provided.
- _PHENOTYPE_NAME_2,3,..._: Presented only if `--make-single` parameter is provided.

Header will not be presented if `--make-separate` parameter is provided.

### Naming

If saving to a single file, it will be named as `${output_prefix}.tsv`, or otherwise
`${output_prefix}/${phenotype_name}.tsv`. `$phenotype_name` is intercepted from the basename
of the input files.
