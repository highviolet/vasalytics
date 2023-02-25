import logging
import json
from pathlib import Path
import asyncio
import aiohttp
import scraping
import pandas as pd

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def download_records():
    """Download record files."""
    async with aiohttp.ClientSession() as session:
        i = 1
        all_records = []
        while True:
            page_records = await scraping.get_page_records(session, i)

            if page_records:
                p_page = Path("page_records") / f"page_{i}.json"
                with open(p_page, "w", encoding="utf-8") as fp:
                    json.dump(page_records, fp, indent=4)
                all_records.extend(page_records)
            else:
                break
            i += 1


def records_to_raw_df():
    p_dir = Path("page_records")

    all_records = []
    for p_file in p_dir.iterdir():
        print(p_file)
        with open(p_file, "r", encoding="utf-8") as fp:
            data = json.load(fp)
            all_records.extend(data)

    df = pd.DataFrame(all_records)
    df.to_pickle("raw.pkl")


def clean_frame(path):
    df = pd.read_pickle("raw.pkl")

    # Time columns
    for col in df.columns:
        if col == "time" or col.startswith("split"):
            df[col] = pd.to_timedelta(df[col], errors="coerce")

    # Fix datatype
    df.start_group = pd.to_numeric(
        df.start_group.str.lstrip("VL"), errors="coerce"
    ).astype("Int64")

    df.place = pd.to_numeric(df.place, errors="coerce").astype("Int64")
    df.place_nosex = pd.to_numeric(df.place_nosex, errors="coerce").astype("Int64")

    # New columns
    df["did_start"] = df.race_status.isin(["Finished", "Did Not Finish", "Started"]).sum()
    df["did_finish"] = df.race_status == "Finished"

    df.to_pickle("clean.pkl")


if __name__ == "__main__":
    # main()
    # asyncio.run(async_main())
    # records_to_raw_df()
    clean_frame("raw.pkl")
