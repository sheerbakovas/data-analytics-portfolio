from pathlib import Path
import json
import os

import pandas as pd
import yagmail
from dotenv import load_dotenv


# Пути
ROOT = Path(__file__).resolve().parents[1]
EVENTS_PATH = ROOT / "data" / "processed" / "events.parquet"
STATE_PATH = ROOT / "state.json"


def read_state_last_hour() -> pd.Timestamp | None:
    """
    Возвращает last_sent_hour из state.json или None, если там null или файла нет.
    """
    if not STATE_PATH.exists():
        return None

    with open(STATE_PATH, "r", encoding="utf-8") as f:
        state = json.load(f)

    value = state.get("last_sent_hour")
    if value is None:
        return None

    return pd.to_datetime(value)


def write_state_last_hour(hour: pd.Timestamp) -> None:
    """
    Записывает last_sent_hour в state.json
    """
    payload = {"last_sent_hour": pd.to_datetime(hour).isoformat()}
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def pick_hour_to_process(df: pd.DataFrame) -> pd.Timestamp:
    """
    - если state пустой -> берём самый ранний час в данных
    - иначе -> следующий час после last_sent_hour
    """
    min_hour = pd.to_datetime(df["event_hour"].min())
    last = read_state_last_hour()

    if last is None:
        return min_hour

    return pd.to_datetime(last) + pd.Timedelta(hours=1)


def compute_metrics(df: pd.DataFrame, hour: pd.Timestamp) -> dict:
    """
    Считает метрики за один час. Если данных нет — возвращает нули.
    """
    hour = pd.to_datetime(hour)
    df_h = df[df["event_hour"] == hour]

    events_total = len(df_h)
    unique_users = int(df_h["UserID"].nunique()) if events_total else 0
    unique_sessions = int(df_h["SessionID"].nunique()) if events_total else 0

    purchases = int(df_h["is_purchase"].sum()) if events_total else 0
    revenue = float(df_h["revenue"].sum()) if events_total else 0.0
    aov = revenue / purchases if purchases > 0 else 0.0

    add_to_cart = int((df_h["EventType"] == "add_to_cart").sum()) if events_total else 0
    product_view = int((df_h["EventType"] == "product_view").sum()) if events_total else 0

    conv_cart_to_purchase = purchases / add_to_cart if add_to_cart > 0 else 0.0
    conv_view_to_purchase = purchases / product_view if product_view > 0 else 0.0

    return {
        "hour": hour,
        "events_total": events_total,
        "unique_users": unique_users,
        "unique_sessions": unique_sessions,
        "purchases": purchases,
        "revenue": revenue,
        "aov": aov,
        "add_to_cart": add_to_cart,
        "product_view": product_view,
        "conv_cart_to_purchase": conv_cart_to_purchase,
        "conv_view_to_purchase": conv_view_to_purchase,
    }


def build_email(metrics: dict) -> tuple[str, str]:
    hour = metrics["hour"]
    hour_start = hour.strftime("%Y-%m-%d %H:%M")
    hour_end = (hour + pd.Timedelta(hours=1)).strftime("%Y-%m-%d %H:%M")

    subject = f"Hourly E-commerce Report — {hour_start}"

    body = (
        f"Hourly E-commerce Report\n"
        f"Period: {hour_start} — {hour_end}\n\n"
        f"Activity\n"
        f"- Total events: {metrics['events_total']}\n"
        f"- Unique users: {metrics['unique_users']}\n"
        f"- Unique sessions: {metrics['unique_sessions']}\n\n"
        f"Sales\n"
        f"- Purchases: {metrics['purchases']}\n"
        f"- Revenue: {metrics['revenue']:.2f}\n"
        f"- AOV: {metrics['aov']:.2f}\n\n"
        f"Funnel\n"
        f"- Add to cart: {metrics['add_to_cart']}\n"
        f"- Product view: {metrics['product_view']}\n"
        f"- Add_to_cart → Purchase: {metrics['conv_cart_to_purchase']:.4f}\n"
        f"- Product_view → Purchase: {metrics['conv_view_to_purchase']:.4f}\n"
    )
    return subject, body


def send_email(subject: str, body: str) -> None:
    load_dotenv()

    sender = os.getenv("SENDER_EMAIL")
    password = os.getenv("SENDER_PASSWORD")
    receiver = os.getenv("RECEIVER_EMAIL")

    if not sender or not password or not receiver:
        raise ValueError("Заполни .env: SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAIL")

    yag = yagmail.SMTP(user=sender, password=password)
    yag.send(to=receiver, subject=subject, contents=body)


def main() -> None:
    if not EVENTS_PATH.exists():
        raise FileNotFoundError("Нет events.parquet. Сначала запусти: python src/prepare_processed.py")

    df = pd.read_parquet(EVENTS_PATH)

    hour = pick_hour_to_process(df)
    metrics = compute_metrics(df, hour)
    subject, body = build_email(metrics)

    send_email(subject, body)
    write_state_last_hour(hour)

    print("Sent:", subject)


if __name__ == "__main__":
    main()