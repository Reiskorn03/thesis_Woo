import os
import pandas as pd

# Map each subset key to its metadata CSV
META_FILES = {
    "cand_kipa":     "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/CAND_KIPA_VER1.csv",
    "pra_hist":      "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/PRA_HIST_VER1.csv",
    "stathist_kipa": "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/STAHIST_KIPA_VER1.csv",
    "tx_ki":         "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/TX_KI_VER1.csv",
    "txf_ki":        "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/TXF_KI_VER1.csv",
}

def classify_feature(row):
    dtype = str(row["Type"]).strip().lower()
    length = int(row["Length"])
    fmt   = str(row["Format"]).strip()
    if dtype == "num":
        return "a_continuous_numeric" if not fmt else "b_numeric_with_format"
    elif dtype.startswith("char"):
        if fmt:
            return "e_char_with_encoding"
        return "c_char_flag" if length == 1 else "d_char_free_text"
    else:
        return "unknown"

all_meta = []
for subset_key, meta_csv in META_FILES.items():
    df_meta = pd.read_csv(meta_csv, dtype=str)[
        ["Variable", "Type", "Length", "Format"]
    ].drop_duplicates().copy()
    df_meta["group"] = df_meta.apply(classify_feature, axis=1)
    df_meta["subset"] = subset_key
    all_meta.append(df_meta)

# concatenate all subsets into one meta DataFrame
meta_df = pd.concat(all_meta, ignore_index=True)

# write out to disk for later reference
out_path = "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/feature_groups_meta.csv"
meta_df.to_csv(out_path, index=False)
print(f"Wrote feature grouping metadata to {out_path}")
