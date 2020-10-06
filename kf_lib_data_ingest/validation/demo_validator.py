import glob
import os

from kf_lib_data_ingest.common.io import read_df
from kf_lib_data_ingest.validation.data_validator import (
    NA,
    Validator,
)
from kf_lib_data_ingest.etl.configuration.log import init_logger

init_logger(log_level="DEBUG")


def message_from_result(result, width):
    description = result["description"]
    valid = result["is_applicable"]
    errors = result["errors"]
    name = "Test: " + description
    status = "🕳️" if not valid else "❌" if errors else "✅"
    message = [f"{status} {name}"]
    if errors:
        parts = {}
        if result["type"] == "relationship":
            parts["Error Reasons"] = [
                f"{e['from']} -> {sorted(e['to'])}" for e in errors
            ]
            parts["Locations"] = list(
                {
                    f"{k} is in {v}"
                    for e in errors
                    for k, v in e["locations"].items()
                }
            )
            for k, v in parts.items():
                message.append(f"\n{k}:")
                for vx in sorted(v):
                    message.append(f"\t{vx}")
        elif result["type"] == "attribute":
            for k, v in errors.items():
                message.append(f"\n{k} contains bad values:")
                for vx in sorted(v):
                    message.append(f'\t"{vx}"')

    return f"\n{'~' * width}\n\n" + "\n".join(message)


# DIRS = sorted(glob.glob("DATASET*"))
DIRS = [
    "../kf-ingest-packages/kf_ingest_packages/packages/SD_BHJXBDQK/output/ExtractStage"
]

for dir in DIRS:
    df_dict = {}
    for f in glob.glob(f"{dir}/*"):
        try:
            fname = os.path.basename(f)
            df_dict[fname] = read_df(f, encoding="utf-8-sig").replace("NA", NA)
        except Exception:
            continue

    if df_dict:
        print(f"## {dir}\n\n```text")
        spaced_dir = f" {dir} "
        divider = 90 * "="
        first_bar_width = (len(divider) - len(spaced_dir)) // 2
        second_bar_width = len(divider) - len(spaced_dir) - first_bar_width
        print(divider)
        print(f"{'=' * first_bar_width}{spaced_dir}{'=' * second_bar_width}")
        print(f"{divider}\n")

        results = Validator().validate(df_dict)

        print("Loaded files:")
        for f in results["files_validated"]:
            print(os.path.join(dir, f))
        print()

        count_block = ["NODE TYPE COUNTS:"]
        counts = results["counts"]
        colwidth = len(max(counts.keys(), key=len))
        count_block += [f"\t{k:<{colwidth}} : {v}" for k, v in counts.items()]
        print("\n".join(count_block))

        for r in results["validation"]:
            print(message_from_result(r, len(divider)))

        print("```")
    else:
        print(f"{dir} not found or contains no data files.")

    print()
