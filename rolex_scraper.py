import concurrent.futures
import logging

import bs4
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.chrono24.com/"
BASE_ENDPOINT = "rolex/index.htm?man=rolex&pageSize=120&resultview=list&showpage="


def _get_soup(url: str) -> bs4.BeautifulSoup:
    HEADERS = {
        "User-Agent": """Mozilla/5.0 (Windows NT 10.0; Win64; x64) 
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"""
    }
    with requests.get(url=url, headers=HEADERS) as response:
        response.raise_for_status()
        return bs4.BeautifulSoup(markup=response.text, features="html.parser")


def _get_number_of_pages_to_scrape() -> int:
    soup = _get_soup(url=f"{BASE_URL}{BASE_ENDPOINT}")
    pagination = soup.find(name="ul", attrs={"class": "pagination"})
    page_number_selectors = [a_tag.get_text(strip=True) for a_tag in pagination.find_all("a")]
    return max([int(num) for num in page_number_selectors if num.isnumeric()])


def _get_page_listings(soup: bs4.BeautifulSoup) -> list[bs4.element.Tag]:
    return soup.find(name="div", attrs={"id": "wt-watches"}).find_all(
        name="div", attrs={"class": "media-flex-body d-flex flex-column justify-content-between p-y-2 p-y-sm-0"}
    )


def _get_listing_name(listing: bs4.element.Tag) -> str:
    return listing.find(name="div", attrs={"class": "text-sm text-sm-xlg text-bold text-ellipsis"}).get_text(strip=True)


def _get_listing_info(listing: bs4.element.Tag) -> dict[str, str]:
    info_table = listing.find(name="div", attrs={"class": "d-none d-sm-flex flex-wrap m-b-3"})
    info_items = [
        tuple(info.get_text(strip=True).split(":", maxsplit=1)) for info in info_table.find_all("div", {"class": "w-50"})
    ]
    return dict(info_items)


def _get_listing_price(listing: bs4.element.Tag) -> str:
    return listing.find("div", {"class": "text-md text-sm-xlg text-bold"}).get_text(strip=True)


def _scrape_page(page_num: int) -> list[dict[str, str]]:
    logger.info(f"Scraping page {page_num}")
    soup = _get_soup(url=f"{BASE_URL}{BASE_ENDPOINT}{page_num}")
    result = [
        {"name": _get_listing_name(listing=listing)}
        | _get_listing_info(listing=listing)
        | {"price": _get_listing_price(listing=listing)}
        for listing in _get_page_listings(soup=soup)
    ]
    logger.info(f"Finished scraping page {page_num}")
    return result


def watch_scraping_program(max_threading_workers: int = 25) -> None:
    start_time = pd.Timestamp.now()
    logger.info(f"Starting watch scraping program as of {pd.Timestamp.now().strftime('%D %T')} with {max_threading_workers=}")
    num_pages = _get_number_of_pages_to_scrape()
    logger.info(f"Found {num_pages} pages to scrape from {BASE_URL}")

    watches = list()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threading_workers) as executor:
        for result in executor.map(_scrape_page, range(1, num_pages + 1)):
            watches += result

    pd.DataFrame(watches).to_csv(f"watches_{pd.Timestamp.today().date()}.csv", index=False)
    timedelta = (pd.Timestamp.now() - start_time).seconds
    logger.info(
        f"Watch scraping program completed successfully, scraping {len(watches)} watches in "
        f"{int(timedelta/60)} minutes and {timedelta % 60} seconds!"
    )


if __name__ == "__main__":
    watch_scraping_program()
