import argparse
import os
import re
import sys

parser = argparse.ArgumentParser(
    description="Extract columns of a csv file"
)
parser.add_argument(
    "--input", "-i", required=True,
    help="Target csv / tsv from which you want to extract certain columns."
)
parser.add_argument(
    "--keep", "-k",
    help="Path to the file which contains columns headers that you want to keep. "
    "Each line of this file contains only one of the headers' name."
    "It is ok that the line is part of the target header"
)
parser.add_argument(
    "--output", "-o",
    required=True,
    help="Output file path where the extracted columns will be saved."
)

if __name__ == "__main__":

    args = parser.parse_args()

    if not os.path.exists(args.input):
        parser.error(f"{os.path.realpath(args.input)} does not exist!")
    if not os.path.exists(args.keep):
        parser.error(f"{os.path.realpath(args.keep)} does not exist!")

    import polars as pl

    print("Loading headers from source...")
    original_headers = pl.read_csv(
        args.input,
        n_rows=0,
        separator="\t" if args.input.endswith(".tsv") else ",",
        infer_schema=False
    ).columns

    print("Loading headers from target file...")
    with open(args.keep) as reader:
        target_headers = list(map(lambda x: x.strip(), reader.readlines()))

    print("Filtering headers")
    kept_headers: list[str] = []
    for original_header in original_headers:
        for target_header in target_headers:
            pattern = re.compile(rf"^.+({target_header}|id).*$")
            if re.match(pattern, original_header):
                kept_headers.append(original_header)
                break

    # print(f"Original: {original_headers[2500:2550]}")
    # print(f"Target: {target_headers[:50]}")
    print(f"Original: {original_headers[:10]}")
    print(f"Selected: {kept_headers}")

    # sys.exit(0)

    print("Scanning csv...")
    lf = pl.scan_csv(
        args.input,
        separator="\t" if args.input.endswith(".tsv") else ",",
        infer_schema=False,
        null_values="NA",
    ).select(kept_headers)

    print("Processing and writing result...")
    lf.sink_csv(
        args.output,
        separator="\t" if args.output.endswith(".tsv") else ",",
        include_header=True,

    )

    # schema_override_dict = {header: pl.Utf8 for header in kept_headers}

    # df_reader = pl.read_csv_batched(
    #     args.input,
    #     separator="\t" if args.input.endswith(".tsv") else ",",
    #     columns=kept_headers,
    #     null_values="NA",
    #     schema_overrides=schema_override_dict,
    #     infer_schema_length=0
    # )

    # print("Write first batch with header")
    # df = df_reader.next_batches(1)[0] # type: ignore

    # df.write_csv(
    #     args.output,
    #     separator="\t" if args.output.endswith(".tsv") else ",",
    #     include_header=True,
    # )

    # print("Write rest of the batches without header")
    # while batches := df_reader.next_batches(5):
    #     for df in batches: # type: ignore
    #         df.write_csv(
    #             args.output,
    #             separator="\t" if args.output.endswith(".tsv") else ",",
    #             include_header=True,
    #         )
    #         # Error: csv will be overwritten!
