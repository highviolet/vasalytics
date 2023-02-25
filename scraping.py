import aiohttp
import logging
from requests_toolbelt.multipart.encoder import MultipartEncoder
from bs4 import BeautifulSoup
import asyncio

logger = logging.getLogger(__name__)


async def get_page_records(session: aiohttp.ClientSession, page: int = 1):
    logger.info("Page: %s", page)
    page_html = await _request_result_list_page(session, page)

    # Parse results table
    soup = BeautifulSoup(page_html, features="lxml")

    # Check for alert (end)
    alerts = soup.select("div.alert")
    if alerts:
        if alerts[0].text.strip() == "No results found.":
            return []

    # Iterate over entries
    results: list[dict] = []
    async with asyncio.TaskGroup() as tg:
        for i, elem in enumerate(soup.select("li.row.list-group-item")[1:]):
            logger.debug("List item %s", i)
            name = elem.select("h4.type-fullname")[0].text
            time = list(elem.select("div.type-time")[0])[1].text

            # Get a link to the details page
            link = elem.select("div a[href]")[0].attrs["href"]
            tg.create_task(_get_record(session, results, link, name, time))

    return results


async def _get_record(session, results: list, link, name, time) -> dict:
    det_response = await _request_details(session, link)
    det_soup = BeautifulSoup(det_response, features="lxml")
    record = _parse_details(name, time, det_soup)
    results.append(record)


async def _request_result_list_page(session: aiohttp.ClientSession, page: int = 1):
    url = r"https://results.vasaloppet.se/2023/?pid=search"

    form_data = {
        "event": "VL_9999991678887600000008EZ",
        "num_results": "100",
        "search_sort": "name",
        "page": f"{page}",
    }

    form = aiohttp.FormData()
    for key, value in form_data.items():
        form.add_field(key, value)

    async with session.post(
        url=url,
        data=form,
    ) as response:
        text = await response.text()

    return text


async def _request_details(session: aiohttp.ClientSession, link: str):
    async with session.get("https://results.vasaloppet.se/" + link) as response:
        text = await response.text()
    return text


def _parse_details(name, time, soup):
    age_class = soup.select("td.f-age_class")[0].text
    start_no = soup.select("td.f-start_no_text")[0].text
    start_group = soup.select("td.f-start_group")[0].text
    place = soup.select("td.f-place_all")[0].text
    place_nosex = soup.select("td.f-place_nosex")[0].text
    race_status = soup.select("td.f-__race_status")[0].text

    record = {
        "name": name,
        "time": time,
        "race_status": race_status,
        "age_class": age_class,
        "start_no": start_no,
        "start_group": start_group,
        "place": place,
        "place_nosex": place_nosex,
    }

    for split in soup.select("tr.split"):
        record[f"split_{split.select('th.desc')[0].text}"] = split.select("td.time")[
            0
        ].text
    return record
