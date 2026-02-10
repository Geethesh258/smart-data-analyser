import pandas as pd
from typing import Tuple
import numpy as np
import re
from typing import Dict, List, Optional

SQL_RESERVED = {
    "select", "from", "where", "group", "order", "table",
    "insert", "update", "delete", "join", "limit"
}

# --------------------------------------------------
# 1. FILE LOADING
# --------------------------------------------------

def load_file(path: str) -> pd.DataFrame:
    if path.lower().endswith(".csv"):
        return pd.read_csv(path, encoding="utf-8", errors="ignore")
    return pd.read_excel(path)


def load_multiple_files(paths: List[str]) -> Dict[str, pd.DataFrame]:
    return {extract_name(p): load_file(p) for p in paths}


def extract_name(path: str) -> str:
    return path.split("\\")[-1].split(".")[0]


# --------------------------------------------------
# 2. COLUMN NAME STANDARDIZATION (SQL SAFE)
# --------------------------------------------------

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    for col in df.columns:
        clean = (
            str(col).strip().lower()
            .replace(" ", "_")
            .replace("-", "_")
        )
        clean = re.sub(r"[^\w_]", "", clean)

        if clean in SQL_RESERVED:
            clean = f"{clean}_col"

        cols.append(clean)

    df.columns = cols
    return df


# --------------------------------------------------
# 3. TEXT CLEANING
# --------------------------------------------------

def clean_text(series: pd.Series, case: str = "title") -> pd.Series:
    s = (
        series.astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    if case == "upper":
        return s.str.upper()
    if case == "lower":
        return s.str.lower()

    return s.str.title()


# --------------------------------------------------
# 4. NULL HANDLING STRATEGIES
# --------------------------------------------------

def handle_nulls(series: pd.Series, strategy: str):
    if strategy == "zero":
        return series.fillna(0)
    if strategy == "mean":
        return series.fillna(series.mean())
    if strategy == "median":
        return series.fillna(series.median())
    if strategy == "mode":
        return series.fillna(series.mode().iloc[0])
    if strategy == "ffill":
        return series.fillna(method="ffill")
    if strategy == "keep":
        return series
    return series.fillna("Unknown")


# --------------------------------------------------
# 5. TYPE DETECTION & ENFORCEMENT
# --------------------------------------------------

def enforce_type(series: pd.Series, target: str):
    if target == "numeric":
        return pd.to_numeric(series, errors="coerce")
    if target == "date":
        return pd.to_datetime(series, errors="coerce", infer_datetime_format=True)
    return series.astype(str)


# --------------------------------------------------
# 6. DUPLICATE HANDLING
# --------------------------------------------------

def remove_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    keep: str = "first"
) -> pd.DataFrame:
    return df.drop_duplicates(subset=subset, keep=keep)


# --------------------------------------------------
# 7. FULL CLEANING PIPELINE (INTELLIGENT)
# --------------------------------------------------
def clean_dataset(
    df: pd.DataFrame,
    column_rules: Optional[Dict] = None
) -> Tuple[pd.DataFrame, Dict]:


    report = {
        "rows_before": len(df),
        "nulls_fixed": {},
        "types_converted": [],
        "duplicates_removed": 0
    }

    df = df.copy()
    df = standardize_column_names(df)

    # Remove fully blank rows
    df.dropna(how="all", inplace=True)

    for col in df.columns:
        rules = column_rules.get(col, {}) if column_rules else {}

        if rules.get("skip_cleaning"):
            continue

        # Type enforcement
        target_type = rules.get("type")

        if target_type:
            df[col] = enforce_type(df[col], target_type)
            report["types_converted"].append(col)
        else:
            # Auto-detect
            df[col] = pd.to_numeric(df[col], errors="ignore")

        # Text cleaning
        if df[col].dtype == "object" and "email" not in col and "url" not in col:
            df[col] = clean_text(df[col], rules.get("case", "title"))

        # Null handling
        nulls_before = df[col].isna().sum()
        if nulls_before > 0:
            strategy = rules.get("null_strategy", "unknown")
            df[col] = handle_nulls(df[col], strategy)
            report["nulls_fixed"][col] = nulls_before

    # Duplicate removal
    before = len(df)
    df = remove_duplicates(df)
    report["duplicates_removed"] = before - len(df)

    df.reset_index(drop=True, inplace=True)
    report["rows_after"] = len(df)

    return df, report


# --------------------------------------------------
# 8. MULTI-DATASET OPERATIONS
# --------------------------------------------------

def append_datasets(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(dfs, ignore_index=True)


def merge_datasets(
    left: pd.DataFrame,
    right: pd.DataFrame,
    key: str,
    how: str = "inner"
) -> pd.DataFrame:
    return pd.merge(left, right, on=key, how=how)


def create_excel_workbook(dfs: Dict[str, pd.DataFrame], output_path: str):
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for name, df in dfs.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)


# --------------------------------------------------
# 9. SQL / POSTGRES SAFETY VALIDATION
# --------------------------------------------------

def sql_safety_report(df: pd.DataFrame) -> List[str]:
    issues = []

    for col in df.columns:
        if df[col].dtype == "object":
            if df[col].astype(str).str.contains("\x00", regex=False).any():
                issues.append(f"Invalid character in column: {col}")

        if df[col].isnull().any():
            issues.append(f"Remaining NULLs in column: {col}")

    return issues
