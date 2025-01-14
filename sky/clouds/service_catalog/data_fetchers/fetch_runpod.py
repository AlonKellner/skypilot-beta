"""A script that generates the RunPod catalog.
This script uses the RunPod graphql api to query the list and real-time prices of the
machines offered by RunPod. The script takes less than a second to run.
"""

import requests
import json
from pathlib import Path
import pandas as pd
from sky.provision.runpod.utils import GPU_NAME_MAP
from sky.clouds.service_catalog.constants import CATALOG_DIR, CATALOG_SCHEMA_VERSION


REGIONS = ["CA", "CZ", "IS", "NL", "NO", "RO", "SE", "US"]
ENDPOINT = "https://api.runpod.io/graphql"


def get_raw_runpod_prices(is_secure: bool) -> pd.DataFrame:
    # Create the GraphQL query to fetch both GPU types and their metadata in one request
    payload = json.dumps(
        {
            "query": f"""
query {{
    gpuTypes {{
        id
        displayName
        memoryInGb
        maxGpuCount
        lowestPrice(input: {{
            gpuCount: 1,
            minDisk: 0,
            minMemoryInGb: 8,
            minVcpuCount: 2,
            secureCloud: {'true' if is_secure else 'false'},
            compliance: null,
            dataCenterId: null,
            globalNetwork: false
        }}) {{
            minimumBidPrice
            uninterruptablePrice
            minVcpu
            minMemory
            stockStatus
            compliance
        }}
        securePrice
        communityPrice
        oneMonthPrice
        oneWeekPrice
        threeMonthPrice
        sixMonthPrice
        secureSpotPrice
    }}
}}
"""
        }
    )

    headers = {"content-type": "application/json"}

    response = requests.post(ENDPOINT, headers=headers, data=payload)
    response_data = response.json()

    # Create a dataframe with GPU metadata for all types
    df = pd.DataFrame(response_data["data"]["gpuTypes"])
    df = pd.concat(
        [
            df.drop("lowestPrice", axis=1),
            pd.json_normalize(df["lowestPrice"]).add_prefix("lowestPrice."),
        ],
        axis=1,
    )
    return df


def get_partial_runpod_catalog(is_secure: bool) -> pd.DataFrame:
    # Get basic runpod machine details
    raw_runpod = get_raw_runpod_prices(is_secure=is_secure)
    runpod = raw_runpod[raw_runpod["id"] != "unknown"].copy()
    runpod = runpod.rename(
        columns={
            "lowestPrice.uninterruptablePrice": "Price",
            "lowestPrice.minimumBidPrice": "SpotPrice",
            "lowestPrice.minVcpu": "vCPUs",
            "lowestPrice.minMemory": "MemoryGiB",
        }
    )

    # Convert runpod ids to skypilot accelerator names
    REVERSE_GPU_MAP = {v: k for k, v in GPU_NAME_MAP.items()}
    runpod_ids = set(runpod["id"].unique())
    mapping_ids = set(REVERSE_GPU_MAP.keys())
    missing_ids = runpod_ids - mapping_ids
    extra_ids = mapping_ids - runpod_ids
    if len(missing_ids) > 0:
        print(f"WARNING! Some machine ids from runpod api were missing from runpod mapping: {missing_ids}")
    if len(extra_ids) > 0:
        print(f"WARNING! Some machine ids in runpod mapping do not exist in runpod api: {extra_ids}")
    runpod["AcceleratorName"] = runpod["id"].replace(REVERSE_GPU_MAP)

    # Duplicate each row for all possible accelerator counts (up to max)
    runpod["AcceleratorCount"] = runpod["maxGpuCount"].apply(
        lambda x: [i + 1 for i in range(x)]
    )
    runpod_exploded = runpod.explode("AcceleratorCount").reset_index(drop=True)
    runpod_exploded["InstanceType"] = (
        runpod_exploded["AcceleratorCount"].astype(str)
        + "x_"
        + runpod_exploded["AcceleratorName"]
    )
    
    def format_gpu_info(row):
        return repr({
            "Gpus": [
                {
                    "Name": row["AcceleratorName"],
                    "Count": str(float(row["AcceleratorCount"])),
                    "MemoryInfo": {"SizeInMiB": row["memoryInGb"] * 1024},
                    "TotalGpuMemoryInMiB": row["AcceleratorCount"] * row["memoryInGb"] * 1024,
                }
            ]
        })
    
    runpod_exploded["GpuInfo"] = runpod_exploded.apply(format_gpu_info, axis="columns")

    # Multiply linearly scaled values by the accelerator count
    for c in ["Price", "SpotPrice", "vCPUs", "MemoryGiB"]:
        runpod_exploded[c] = runpod_exploded[c] * runpod_exploded["AcceleratorCount"]

    # Duplicate each row for all runpod regions
    runpod_exploded["Region"] = runpod_exploded["id"].apply(lambda x: REGIONS)
    runpod_ex_exploded = runpod_exploded.explode("Region").reset_index(drop=True)

    # Filter & Reorder dataframe columns to match the catalog scheme
    formatted_runpod = runpod_ex_exploded[
        [
            "InstanceType",
            "AcceleratorName",
            "AcceleratorCount",
            "vCPUs",
            "MemoryGiB",
            "GpuInfo",
            "Region",
            "SpotPrice",
            "Price",
        ]
    ]
    return formatted_runpod


def get_runpod_catalog() -> pd.DataFrame:
    secure_prices = get_partial_runpod_catalog(is_secure=True)
    secure_prices["InstanceType"] = secure_prices["InstanceType"] + "_SECURE"
    community_prices = get_partial_runpod_catalog(is_secure=False)
    community_prices["InstanceType"] = community_prices["InstanceType"] + "_COMMUNITY"
    return pd.concat([secure_prices, community_prices], axis=0).reset_index(drop=True)


def update_runpod_catalog(catalog_path: str):
    sky_runpod_prices = get_runpod_catalog()
    csv_path = Path(catalog_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    sky_runpod_prices.to_csv(csv_path.absolute(), index=False)
    return sky_runpod_prices


if __name__ == "__main__":
    catalog_path = f"{CATALOG_DIR}/{CATALOG_SCHEMA_VERSION}/runpod/vms.csv"
    update_runpod_catalog(catalog_path)
    print(f"RunPod Service Catalog saved to {catalog_path}")
