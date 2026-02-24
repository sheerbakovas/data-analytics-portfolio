import os
import asyncio
import datetime as dt
from collections import Counter

import requests
import pandas as pd
from dotenv import load_dotenv
from telegram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = int(os.environ["CHAT_ID"])
bot = Bot(token=BOT_TOKEN)

ADZUNA_APP_ID = os.environ["ADZUNA_APP_ID"]
ADZUNA_APP_KEY = os.environ["ADZUNA_APP_KEY"]
ADZUNA_COUNTRY = os.getenv("ADZUNA_COUNTRY", "gb").lower()

COUNTRY_META = {
    "gb": {"name": "United Kingdom", "currency": "GBP", "symbol": "£"},
    "us": {"name": "United States", "currency": "USD", "symbol": "$"},
    "de": {"name": "Germany", "currency": "EUR", "symbol": "€"},
    "fr": {"name": "France", "currency": "EUR", "symbol": "€"},
    "nl": {"name": "Netherlands", "currency": "EUR", "symbol": "€"},
    "ca": {"name": "Canada", "currency": "CAD", "symbol": "C$"},
    "au": {"name": "Australia", "currency": "AUD", "symbol": "A$"},
    "in": {"name": "India", "currency": "INR", "symbol": "₹"},
}

meta = COUNTRY_META.get(ADZUNA_COUNTRY, {})
COUNTRY_NAME = meta.get("name", ADZUNA_COUNTRY.upper())
CURRENCY = meta.get("currency", "")
CURRENCY_SYMBOL = meta.get("symbol", "")

ROLES = [
    "Data Analyst",
    "Product Analyst",
    "BI Analyst",
    "Data Engineer",
    "ML Engineer",
]


def adzuna_search(what: str, page: int = 1, results_per_page: int = 50) -> dict:
    url = f"https://api.adzuna.com/v1/api/jobs/{ADZUNA_COUNTRY}/search/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "what": what,
        "results_per_page": results_per_page,
        "content-type": "application/json",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_salary(job: dict):
    salary_min = job.get("salary_min")
    salary_max = job.get("salary_max")

    if salary_min is None and salary_max is None:
        return None
    if salary_min is None:
        return float(salary_max)
    if salary_max is None:
        return float(salary_min)
    return (float(salary_min) + float(salary_max)) / 2

def extract_location(job: dict):
    loc = job.get("location") or {}

    area = loc.get("area") or []
    display_name = loc.get("display_name")

    if area:
        country = area[0]
        most_specific = area[-1]

        if most_specific and most_specific != country:
            return f"{most_specific}, {country}"

    if display_name:
        if display_name.strip().lower() in {"uk", "united kingdom"}:
            return None
        return display_name

    return None

def get_role_stats(role: str) -> dict:
    data = adzuna_search(what=role, page=1, results_per_page=50)
    results = data.get("results", [])
    total = int(data.get("count", 0))

    salaries = []
    locations = []
    companies = []

    for job in results:
        salary = extract_salary(job)
        if salary is not None:
            salaries.append(salary)

        location = extract_location(job)
        if location:
            locations.append(location)

        company = (job.get("company") or {}).get("display_name")
        if company:
            companies.append(company)

    avg_salary = round(sum(salaries) / len(salaries), 2) if salaries else None
    salary_share = round(len(salaries) / len(results) * 100, 1) if results else 0
    
    top_locations = Counter(locations).most_common(3)
    top_companies = Counter(companies).most_common(3)

    return {
        "role": role,
        "total": total,
        "sample": len(results),
        "avg_salary": avg_salary,
        "salary_share": salary_share,
        "top_locations": top_locations,
        "top_companies": top_companies,
    }

def make_excel_report(rows: list[dict]) -> str:
    today = dt.date.today().isoformat()
    filename = f"adzuna_report_{ADZUNA_COUNTRY}_{today}.xlsx"

    df_roles = pd.DataFrame(rows)

    df_roles["top_locations"] = df_roles["top_locations"].apply(
        lambda items: ", ".join(f"{name} ({count})" for name, count in items) or "нет данных"
    )
    df_roles["top_companies"] = df_roles["top_companies"].apply(
        lambda items: ", ".join(f"{name} ({count})" for name, count in items) or "нет данных"
    )

    df_roles["currency"] = CURRENCY

    df_summary = pd.DataFrame([{
        "date": today,
        "country_code": ADZUNA_COUNTRY,
        "country_name": COUNTRY_NAME,
        "currency": CURRENCY,
        "roles_count": len(rows),
    }])    

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        df_summary.to_excel(writer, sheet_name="Summary", index=False)
        df_roles.to_excel(writer, sheet_name="ByRole", index=False)

    return filename



async def send_daily_report():
    report_lines = [f"Adzuna daily report ({COUNTRY_NAME}) - {dt.date.today().isoformat()}"]
    rows = []

    for role in ROLES:
        stats = get_role_stats(role)

        avg_salary_text = (
            "нет данных" if stats["avg_salary"] is None else f"{stats['avg_salary']:.0f} {CURRENCY}".strip()
        )

        top_locations_text = ", ".join(
            f"{name} ({count})" for name, count in stats["top_locations"]
        ) or "нет данных"

        top_companies_text = ", ".join(
            f"{name} ({count})" for name, count in stats["top_companies"]
        ) or "нет данных"

        report_lines.append(
            f"\n{stats['role']}\n"
            f"total: {stats['total']}\n"
            f"sample: {stats['sample']}\n"
            f"avg salary: {avg_salary_text}\n"
            f"salary share: {stats['salary_share']}%\n"
            f"top locations: {top_locations_text}\n"
            f"top companies: {top_companies_text}"
        )

        rows.append(stats)

    report_text = "\n".join(report_lines)

    filename = make_excel_report(rows)

    await bot.send_message(chat_id=CHAT_ID, text=report_text)

    caption = f"Adzuna report - {dt.date.today().isoformat()} - {COUNTRY_NAME} - xlsx file"
    with open(filename, "rb") as f:
        await bot.send_document(chat_id=CHAT_ID, document=f, caption=caption)

async def main():
    TZ = pytz.timezone("Europe/Moscow")

    scheduler = AsyncIOScheduler(timezone=TZ)

    scheduler.add_job(
        send_daily_report,
        CronTrigger(hour=9, minute=0, timezone=TZ),
        name="Send Adzuna daily report",
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()

    await send_daily_report()

    print("Scheduler started. Waiting for next daily run (09:00 Europe/Moscow)...")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())