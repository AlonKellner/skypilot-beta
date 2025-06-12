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


ZONES_TO_REGIONS = {
    "OC-AU-1": "AU",
    "CA-MTL-1": "CA",
    "CA-MTL-2": "CA",
    "CA-MTL-3": "CA",
    "CA-MTL-4": "CA",
    "EU-CZ-1": "CZ",
    "EU-FR-1": "FR",
    "AP-JP-1": "JP",
    "EU-NL-1": "NL",
    "EU-RO-1": "RO",
    "EU-SE-1": "SE",
    "EU-SE-2": "SE",
    "EUR-NO-1": "NO",
    "EUR-IS-1": "IS",
    "EUR-IS-2": "IS",
    "EUR-IS-3": "IS",
    "SEA-SG-1": "SG",
    "US-CA-1": "US",
    "US-CA-2": "US",
    "US-DE-1": "US",
    "US-GA-1": "US",
    "US-GA-2": "US",
    "US-IL-1": "US",
    "US-KS-1": "US",
    "US-KS-2": "US",
    "US-KS-3": "US",
    "US-NC-1": "US",
    "US-TX-1": "US",
    "US-TX-2": "US",
    "US-TX-3": "US",
    "US-TX-4": "US",
    "US-TX-5": "US",
    "US-WA-1": "US",
    "US-OR-1": "US",
    "US-OR-2": "US",
    "US-MO-1": "US",
}


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
    securePrice
    communityPrice
    oneMonthPrice
    oneWeekPrice
    threeMonthPrice
    sixMonthPrice
    secureSpotPrice
    lowestPrice(input: {{
        gpuCount: 1,
        minDisk: 0,
        minMemoryInGb: 8,
        minVcpuCount: 2,
        secureCloud: {"true" if is_secure else "false"},
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
  }}
  dataCenters {{
    id
    name
    location
    gpuAvailability {{
      available
      stockStatus
      gpuTypeId
      gpuTypeDisplayName
      displayName
      id
    }}
  }}
}}
"""
        }
    )

    headers = {"content-type": "application/json"}

    response = requests.post(ENDPOINT, headers=headers, data=payload)
    response_data = response.json()
    df = pd.DataFrame(response_data["data"]["dataCenters"])
    df = df.explode("gpuAvailability").reset_index(drop=True)
    df = pd.concat(
        [
            df.drop("gpuAvailability", axis=1),
            pd.json_normalize(df["gpuAvailability"]).add_prefix("gpuAvailability."),
        ],
        axis=1,
    )

    api_zones = set(df["name"].unique())
    expected_zones = set(ZONES_TO_REGIONS.keys())

    missing_zones = api_zones - expected_zones
    if len(missing_zones) > 0:
        print(
            f"WARNING! Some data center names in the RunPod API are not mapped to regions: {missing_zones}. "
        )
    extra_zones = expected_zones - api_zones
    if len(extra_zones) > 0:
        print(
            f"WARNING! Some data center names in ZONES_TO_REGIONS are not present in the RunPod API: {extra_zones}. "
        )

    df["region"] = df["name"].map(ZONES_TO_REGIONS)
    df["gpu_id"] = df["gpuAvailability.id"]

    gpu_df = pd.DataFrame(response_data["data"]["gpuTypes"])
    gpu_df = pd.concat(
        [
            gpu_df.drop("lowestPrice", axis=1),
            pd.json_normalize(gpu_df["lowestPrice"]).add_prefix("lowestPrice."),
        ],
        axis=1,
    )
    gpu_df["gpu_id"] = gpu_df["id"]

    df = df.join(gpu_df.set_index("gpu_id"), on="gpu_id", rsuffix=".gpu")

    return df


def get_partial_runpod_catalog(is_secure: bool) -> pd.DataFrame:
    # Get basic runpod machine details
    raw_runpod = get_raw_runpod_prices(is_secure=is_secure)
    runpod = raw_runpod[raw_runpod["id.gpu"] != "unknown"].copy()
    runpod = runpod.rename(
        columns={
            "lowestPrice.uninterruptablePrice": "Price",
            "lowestPrice.minimumBidPrice": "SpotPrice",
            "lowestPrice.minVcpu": "vCPUs",
            "lowestPrice.minMemory": "MemoryGiB",
            "region": "Region",
            "name": "AvailabilityZone",
        }
    )

    # Convert runpod ids to skypilot accelerator names
    REVERSE_GPU_MAP = {v: k for k, v in GPU_NAME_MAP.items()}
    runpod_ids = set(runpod["id.gpu"].unique())
    mapping_ids = set(REVERSE_GPU_MAP.keys())
    missing_ids = runpod_ids - mapping_ids
    extra_ids = mapping_ids - runpod_ids
    if len(missing_ids) > 0:
        print(
            f"WARNING! Some machine ids from RunPod API were missing from GPU_NAME_MAP: {missing_ids}"
        )
    if len(extra_ids) > 0:
        print(
            f"WARNING! Some machine ids in GPU_NAME_MAP do not exist in RunPod API: {extra_ids}"
        )
    runpod["AcceleratorName"] = runpod["id.gpu"].replace(REVERSE_GPU_MAP)

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
        return repr(
            {
                "Gpus": [
                    {
                        "Name": row["AcceleratorName"],
                        "Count": str(float(row["AcceleratorCount"])),
                        "MemoryInfo": {"SizeInMiB": row["memoryInGb"] * 1024},
                        "TotalGpuMemoryInMiB": row["AcceleratorCount"]
                        * row["memoryInGb"]
                        * 1024,
                    }
                ]
            }
        )

    runpod_exploded["GpuInfo"] = runpod_exploded.apply(format_gpu_info, axis="columns")

    # Multiply linearly scaled values by the accelerator count
    for c in ["Price", "SpotPrice", "vCPUs", "MemoryGiB"]:
        runpod_exploded[c] = runpod_exploded[c] * runpod_exploded["AcceleratorCount"]

    # Filter & Reorder dataframe columns to match the catalog scheme
    formatted_runpod = runpod_exploded[
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
            "AvailabilityZone",
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
