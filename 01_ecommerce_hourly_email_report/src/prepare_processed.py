from __future__ import annotations

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = PROJECT_ROOT / "data" / "raw" / "ecommerce_clickstream_transactions.csv"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OUT_PATH = PROCESSED_DIR / "events.parquet"


REQUIRED_COLUMNS = [
    "UserID",
    "SessionID",
    "Timestamp",
    "EventType",
    "ProductID",
    "Amount",
    "Outcome",
]


def load_raw(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Raw file not found: {path}\n"
            f"Put the dataset CSV into: {PROJECT_ROOT / 'data' / 'raw'}"
        )

    df = pd.read_csv(path)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Raw file is missing required columns: {missing}\n"
            f"Found columns: {list(df.columns)}"
        )

    return df


def prepare_events(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce", utc=False)
    df = df.dropna(subset=["Timestamp"]).copy()

    df["EventType"] = df["EventType"].astype(str).str.strip().str.lower()

    df["event_hour"] = df["Timestamp"].dt.floor("f")

    df["UserID"] = pd.to_numeric(df["UserID"], errors="coerce").astype("Int64")
    df["SessionID"] = pd.to_numeric(df["SessionID"], errors="coerce").astype("Int64")

    df["ProductID"] = df["ProductID"].astype("string")
    df.loc[df["ProductID"].str.lower().isin(["nan", "none"]), "ProductID"] = pd.NA

    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")

    df["is_purchase"] = df["EventType"] == "purchase"

    df["revenue"] = df["Amount"].where(df["is_purchase"], 0.0).fillna(0.0)

    df["Outcome"] = df["Outcome"].astype("string")

    out = df[
        [
            "UserID",
            "SessionID",
            "Timestamp",
            "event_hour",
            "EventType",
            "ProductID",
            "Outcome",
            "is_purchase",
            "revenue",
        ]
    ].copy()

    out = out.dropna(subset=["UserID", "SessionID"]).copy()

    out["UserID"] = out["UserID"].astype("Int64")
    out["SessionID"] = out["SessionID"].astype("Int64")
    out["is_purchase"] = out["is_purchase"].astype(bool)
    out["revenue"] = out["revenue"].astype(float)

    return out


def save_processed(df: pd.DataFrame) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)


def print_summary(df: pd.DataFrame) -> None:
    print(f"Saved: {OUT_PATH}")
    print(f"Rows: {len(df):,}")
    print("Min Timestamp:", df["Timestamp"].min())
    print("Max Timestamp:", df["Timestamp"].max())
    print("Min hour:", df["event_hour"].min())
    print("Max hour:", df["event_hour"].max())
    print("\nEventType counts (top 20):")
    print(df["EventType"].value_counts().head(20).to_string())
    print("\nPurchases:", int(df["is_purchase"].sum()))
    print("Revenue sum:", float(df["revenue"].sum()))


def main() -> None:
    raw = load_raw(RAW_PATH)
    events = prepare_events(raw)
    save_processed(events)
    print_summary(events)


if __name__ == "__main__":
    main()