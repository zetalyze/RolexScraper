import concurrent.futures
import logging

import bs4
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__.split("\\")[-1])


BASE_URL = "https://www.chrono24.com/"
BASE_ENDPOINT = "rolex/index.htm?man=rolex&pageSize=120&resultview=list&showpage="
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}
PARSER = "html.parser"
MAX_THREADING_WORKERS = 16


def _get_number_of_pages_to_scrape() -> int:
    soup = bs4.BeautifulSoup(markup=requests.get(url=f"{BASE_URL}{BASE_ENDPOINT}", headers=HEADERS).text, features=PARSER)
    pagination = soup.find(name="ul", attrs={"class": "pagination"})
    page_number_selectors = [a_tag.get_text(strip=True) for a_tag in pagination.find_all("a")]
    return max([int(num) for num in page_number_selectors if num.isnumeric()])


def _get_page_listings(soup: bs4.BeautifulSoup) -> list[bs4.element.Tag]:
    return soup.find(name="div", attrs={"id": "wt-watches"}).find_all(
        name="div", attrs={"class": "media-flex-body d-flex flex-column justify-content-between"}
    )


def _get_listing_name(listing: bs4.element.Tag) -> str:
    return listing.find(name="div", attrs={"class": "text-xlg text-bold text-ellipsis"}).get_text(strip=True)


def _get_listing_info(listing: bs4.element.Tag) -> dict[str, str]:
    info_table = listing.find(name="div", attrs={"class": "d-none d-sm-flex flex-wrap m-b-2"})
    info_items = [tuple(info.get_text(strip=True).split(":")) for info in info_table.find_all("div", {"class": "w-50"})]
    return dict(info_items)


def _get_listing_price(listing: bs4.element.Tag) -> str:
    return listing.find("div", {"class": "text-xlg text-bold"}).get_text(strip=True)


def _scrape_page(page_num: int) -> list[dict[str, str]]:
    logger.info(f"Scraping page {page_num}")
    soup = bs4.BeautifulSoup(
        markup=requests.get(url=f"{BASE_URL}{BASE_ENDPOINT}{page_num}", headers=HEADERS).text, features=PARSER
    )
    result = [
        {"name": _get_listing_name(listing=listing)}
        | _get_listing_info(listing=listing)
        | {"price": _get_listing_price(listing=listing)}
        for listing in _get_page_listings(soup=soup)
    ]
    logger.info(f"Finished scraping page {page_num}")
    return result


def watch_scraping_program():
    start_time = pd.Timestamp.now()
    logger.info(f"Starting watch scraping program as of {pd.Timestamp.now().strftime('%D %T')}")
    num_pages = _get_number_of_pages_to_scrape()
    logger.info(f"Found {num_pages} pages to scrape from {BASE_URL}")
    watches = list()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADING_WORKERS) as executor:
        for result in executor.map(_scrape_page, range(1, num_pages + 1)):
            watches += result

    pd.DataFrame(watches).to_csv("watches.csv", index=False)
    timedelta = (pd.Timestamp.now() - start_time).seconds
    logger.info(
        f"Watch scraping program completed successfully, scraping {len(watches)} watches in "
        f"{int(timedelta/60)} minutes and {timedelta % 60} seconds!"
    )


if __name__ == "__main__":
    watch_scraping_program()
