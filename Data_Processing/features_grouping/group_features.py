import os
import pandas as pd

# Map each subset key to its metadata CSV
META_FILES = {
    "cand_kipa":     "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/ver1_meta_datasets/CAND_KIPA_VER1.csv",
    "pra_hist":      "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/ver1_meta_datasets/PRA_HIST_VER1.csv",
    "stathist_kipa": "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/ver1_meta_datasets/STAHIST_KIPA_VER1.csv",
    "tx_ki":         "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/ver1_meta_datasets/TX_KI_VER1.csv",
    "txf_ki":        "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/ver1_meta_datasets/TXF_KI_VER1.csv",
}

# For each subset, list exactly the Variables we already dropped
DROP_FEATURES = {
    "cand_kipa": [
        "CAN_LIFE_SUPPORT", "CAN_PAIRED_LIVE_DONOR_ID", 
        "CAN_CMV_STAT", "CAN_C_PEPTIDE",
        "CAN_GROWTH_HORMONE", "CAN_AVN",
        "CAN_CREAT_CLEAR", "CAN_CREAT_CLEAR_DT",
        "CAN_MM_ACPT_CD", "CAN_ANGINA",
        "CAN_PULM_EMBOL", "CAN_DIAB",
        "PERS_SSA_DEATH_DT", 
        "CAN_INIT_INACT_STAT_DT", "CAN_LAST_INACT_STAT_DT",
        "CAN_DIAL_DT", "CAN_INIT_ACT_STAT_CD",
        "CAN_INIT_ACT_STAT_DT", "CAN_LAST_ACT_STAT_DT", 
    ],
    "pra_hist": [
        "CANHX_BEGIN_DT",
    ],      
    "stathist_kipa": [
        "CANHX_END_DT_TM", "CANHX_BEGIN_DT_TM",
        "CAN_INIT_STAT", "CAN_INIT_ACT_STAT_CD",
        "CAN_INIT_INACT_STAT_DT", "CAN_LAST_ACT_STAT_DT",
        "CAN_LAST_INACT_STAT_DT", "CAN_INIT_ACT_STAT_DT",
    ],
    "tx_ki": [
        "REC_COD3", "REC_COD2",
        "REC_ACUTE_REJ_BIOPSY_CONFIRMED", "REC_AVN",
        "DON_HAPLO_TY_MATCH", "REC_ANTIVRL_THERAPY_TY",
        "REC_DIAL_DT", "DON_HIST_CIGARETTE_GT20_PKYR",
        "DON_HIST_COCAINE", "ORG_TY",
        "ORG_AR",
    ],        
    "txf_ki": [
        "TFL_COD2", "TFL_COD3",
        "TFL_BK_THERAPY_TY", "TFL_FAIL_BK",
        "TFL_CAD", "TFL_AVN",
        "TFL_ANTIVRL_THERAPY_TY", "TFL_URINE_PROTEIN",
        "TFL_IMMUNO_DISCONT", 
    ],        
}

def classify_feature(row):
    dtype = row["Type"].strip().lower()
    length = int(row["Length"])
    fmt   = str(row["Format"]).strip().lower()
    if fmt == "nan":
        fmt = ""
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
    df_meta = (
        pd.read_csv(meta_csv, dtype=str)
          .loc[:, ["Variable", "Type", "Length", "Format"]]
          .drop_duplicates()
    )
    # drop the features we already removed from that subset (updating meta dataset from ver1 -> ver2)
    to_drop = DROP_FEATURES.get(subset_key, [])
    if to_drop:
        df_meta = df_meta[~df_meta["Variable"].isin(to_drop)]
    
    # classify and tag
    df_meta["group"]  = df_meta.apply(classify_feature, axis=1)
    df_meta["subset"] = subset_key
    all_meta.append(df_meta)

# concatenate all subsets into one meta DataFrame
meta_df = pd.concat(all_meta, ignore_index=True)

# write out to disk for later reference
out_path = "/Users/chanyoungwoo/Thesis/Data_Extraction/meta_datasets/ver2_meta_dataset/feature_groups_meta.csv"
meta_df.to_csv(out_path, index=False)
print(f"Wrote feature grouping metadata to {out_path}")

# Final Groups:
# A	Pure continuous numerics	
# B	Numerics with a special format	
# C	Single-character flags	
# D	Free-text or long chars (no format)	
# E	Short coded categories (with format)	
