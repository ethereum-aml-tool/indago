import os
import subprocess
import sys
import pandas as pd
from shutil import rmtree
from tqdm import tqdm
from typing import Optional, List
from utils.storage.base import EthereumStorage
from utils.storage.google_cloud_storage import GoogleCloudStorage

# assert (
#     len(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]) > 0
# ), "Set GOOGLE_APPLICATION_CREDENTIALS prior to use, see README.md"
assert (
    len(os.environ["PYTHONPATH"]) > 0
), "Set PYTHONPATH to include this repository prior to use, see README.md"

BQ_TABLE_NAME: str = "traces"
BUCKET_NAME: str = f"indago"
DOWNLOAD_DIR: str = "~/data"
COLUMNS_TO_SAVE: Optional[List[str]] = [
    "block_number",
    "transaction_index",
    "trace_address",
    "from_address",
    "to_address",
    "value",
    "gas_used",
    "status",
]  # 'None' = Download all columns.
DTYPES = {
    "block_number": int,
    "transaction_index": pd.Int32Dtype(),
    "from_address": str,
    "to_address": str,
    "value": float,
    "trace_address": str,
    "gas_used": pd.Int32Dtype(),
    "status": int,
}
# 'None' = Download all blobs to local storage.
# MAX_BLOBS: Optional[int] = None
MAX_BLOBS: Optional[int] = 10

STEP_SIZE: int = 50
START_OFFSET: int = 0  # 6000 #12000 #18000
END_OFFSET: int = START_OFFSET + STEP_SIZE
N_BLOBS: int = 10  # 12000 #18000 #23478
while END_OFFSET < N_BLOBS + STEP_SIZE:
    print("[DOWNLOADING]")
    storage: EthereumStorage = GoogleCloudStorage()
    storage.download(
        BUCKET_NAME,
        f"{BQ_TABLE_NAME}/",
        f"{DOWNLOAD_DIR}/{BQ_TABLE_NAME}",
        MAX_BLOBS,
        use_cols=COLUMNS_TO_SAVE,
        dtypes=DTYPES,
        start_offset=START_OFFSET,
        end_offset=END_OFFSET,
    )

    print("\n[SORTING]")
    COLUMN_TO_SORT: int = 1
    SEC_COLUMN_TO_SORT: int = 2
    CORES: int | None = os.cpu_count()
    for file in tqdm(os.listdir(f"{DOWNLOAD_DIR}/{BQ_TABLE_NAME}")):
        path: str = f"{DOWNLOAD_DIR}/{BQ_TABLE_NAME}/{file}"
        if (
            subprocess.call(
                f"tail -n+2 {path} | LC_ALL=C sort -t',' --parallel={CORES} -k {COLUMN_TO_SORT},{COLUMN_TO_SORT}n -k {SEC_COLUMN_TO_SORT},{SEC_COLUMN_TO_SORT}n > {DOWNLOAD_DIR}/{BQ_TABLE_NAME}/sorted-{file}",
                shell=True,
            )
            != 0
        ):
            print(f"ERROR: Failed to sort {file} {BQ_TABLE_NAME}")
            sys.exit(1)
        os.remove(f"{DOWNLOAD_DIR}/{BQ_TABLE_NAME}/{file}")

    print("\n[UPLOADING]")
    if (
        subprocess.call(
            [
                "gsutil",
                "-m",
                "cp",
                "-r",
                f"{DOWNLOAD_DIR}/{BQ_TABLE_NAME}",
                f"gs://{BUCKET_NAME}/pruned/",
            ]
        )
        == 0
    ):
        print(f"Uploaded pruned {BQ_TABLE_NAME}!")
        rmtree(f"{DOWNLOAD_DIR}/{BQ_TABLE_NAME}")
        START_OFFSET += STEP_SIZE
        END_OFFSET += STEP_SIZE
    else:
        print(f"ERROR: Failed to upload {BQ_TABLE_NAME}")
        sys.exit(1)

subprocess.call(
    ["gsutil", "-m", "cp", "-r", f"gs://{BUCKET_NAME}/pruned/", DOWNLOAD_DIR]
)
print(f"Downloaded {BQ_TABLE_NAME} to {DOWNLOAD_DIR}")

# Execute bash script to sort the downloaded files.
print("\n[SORTING]")
if subprocess.call(["bash", "scripts/bash/sort_csv_traces.sh"]) == 0:
    print(f"Successfully sorted {BQ_TABLE_NAME}")
else:
    print(f"ERROR: Failed to sort {BQ_TABLE_NAME}")
    sys.exit(1)

print("\n[UPLOADING]")
if (
    subprocess.call(
        [
            "gsutil",
            "-m",
            "cp",
            "-r",
            f"{DOWNLOAD_DIR}/pruned/traces/traces-sorted.csv",
            f"gs://{BUCKET_NAME}/sorted/",
        ]
    )
    == 0
):
    print(f"Uploaded sorted {BQ_TABLE_NAME}!")
else:
    print(f"ERROR: Failed to upload {BQ_TABLE_NAME}")
    sys.exit(1)
