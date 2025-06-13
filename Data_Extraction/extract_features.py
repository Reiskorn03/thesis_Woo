"""
extract_features.py

It loads from the raw SAS files only the features defined in the metadata CSV, 
and saves a separate subset dataset as a CSV for each file.
"""

import os
import pandas as pd
import pyreadstat

# Mapping: What .csv File is linked to which SAS file?
META_FILES = {
    "cand_kipa":    "meta_datasets/CAND_KIPA Input.csv",
    "pra_hist":     "meta_datasets/PRA_HIST Input.csv",
    "stathist_kipa":"meta_datasets/STATHIST_KIPA Input.csv",
    "tx_ki":        "meta_datasets/TX_KI Input.csv",
    "txf_ki":       "meta_datasets/TXF_KI Output.csv",
}

# SAS_Folder path
SAS_FOLDER = "/Users/chanyoungwoo/Thesis/EDA/pubsaf_extracted"

# Folder for Subsets
OUT_FOLDER = "extracted_subsets"
os.makedirs(OUT_FOLDER, exist_ok=True)

def load_feature_lists():
    """
    Loads only the features defined in the metadata CSV from the raw SAS files 
    and saves a separate subset dataset as a CSV for each file.
    Reads each metadata CSV and constructs:
    feature_lists[file_key] = [<Feature1>, <Feature2>, …]
    """
    feature_lists = {}
    for key, csvfile in META_FILES.items():
        df_meta = pd.read_csv(csvfile)
        # delete Duplicate
        features = (
            df_meta
            .loc[df_meta["Feature"].notna(), "Feature"]
            .astype(str)
            .unique()
            .tolist()
        )
        feature_lists[key] = features
    return feature_lists

def read_sas_subset(sas_name, usecols):
    """
    Reads a SAS7BDAT file from SAS_FOLDER, 
    selects only the columns specified in usecols, 
    and returns a pandas.DataFrame.
    """
    path = os.path.join(SAS_FOLDER, sas_name + ".sas7bdat")
    print(f"Read {sas_name}.sas7bdat with {len(usecols)} columns…")
    df, meta = pyreadstat.read_sas7bdat(path, usecols=usecols)
    return df

def main():
    print("Load Feature List…")
    feature_lists = load_feature_lists()

    for key, features in feature_lists.items():
        # Read SAS
        subset_df = read_sas_subset(key, features)

        # Save as .csv
        out_path = os.path.join(OUT_FOLDER, f"{key}_subset.csv")
        print(f"Save subset: {out_path}")
        subset_df.to_csv(out_path, index=False)

    print("All subsets extracted")

if __name__ == "__main__":
    main()
