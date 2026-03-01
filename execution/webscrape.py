import requests
import pandas as pd
import os
from time import sleep


def _is_pdf_headers(headers: dict[str, str]) -> bool:
    content_type = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
    content_disposition = (headers.get("Content-Disposition") or headers.get("content-disposition") or "").lower()
    return "application/pdf" in content_type or ".pdf" in content_disposition


def _build_urls(date_range: list[str]) -> list[str]:
    month_range = pd.date_range(start=date_range[0], end=date_range[1], freq="ME").strftime("%B_%Y")
    return [f"https://www.acea.auto/files/Press_release_car_registrations_{month}.pdf" for month in month_range]


def _save_bytes(directory: str, filename: str, file_bytes: bytes) -> None:
    filepath = os.path.join(directory, filename)
    with open(filepath, "wb") as file_handle:
        file_handle.write(file_bytes)


def _download_with_requests(urls: list[str], directory: str, sleep_seconds: float) -> tuple[list[str], int]:
    failed_urls: list[str] = []
    downloaded_count = 0

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
            "Referer": "https://www.acea.auto/",
        }
    )

    try:
        for url in urls:
            response = session.get(url, timeout=20, stream=True)
            if response.status_code == 200 and _is_pdf_headers(dict(response.headers)):
                filename = url.split("/")[-1]
                filepath = os.path.join(directory, filename)
                with open(filepath, "wb") as file_handle:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file_handle.write(chunk)
                downloaded_count += 1
                print(f"Downloaded {filename} via requests")
            else:
                failed_urls.append(url)
                content_type = response.headers.get("Content-Type", "unknown")
                print(f"Failed {url}: status={response.status_code}, content-type={content_type}")
            sleep(sleep_seconds)
    finally:
        session.close()

    return failed_urls, downloaded_count


def _download_with_playwright(urls: list[str], directory: str, sleep_seconds: float, headless: bool) -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Playwright is not installed. Install with: pip install playwright ; playwright install chromium")
        return 0

    downloaded_count = 0
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        page.goto("https://www.acea.auto/", wait_until="domcontentloaded", timeout=60000)

        for url in urls:
            response = context.request.get(url, timeout=60000)
            headers = response.headers
            if response.status == 200 and _is_pdf_headers(headers):
                filename = url.split("/")[-1]
                _save_bytes(directory, filename, response.body())
                downloaded_count += 1
                print(f"Downloaded {filename} via playwright")
            else:
                content_type = headers.get("content-type", "unknown")
                print(f"Playwright failed {url}: status={response.status}, content-type={content_type}")
            sleep(sleep_seconds)

        context.close()
        browser.close()

    return downloaded_count


def _download_with_playwright_manual(urls: list[str], directory: str, sleep_seconds: float, wait_seconds: float) -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Playwright is not installed. Install with: pip install playwright ; playwright install chromium")
        return 0

    downloaded_count = 0
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        page.goto("https://www.acea.auto/", wait_until="domcontentloaded", timeout=60000)

        print("Manual mode: complete any challenge in the opened browser window.")
        try:
            input("When done, return here and press Enter to continue downloads... ")
        except EOFError:
            print(f"No interactive terminal input detected. Waiting {wait_seconds} seconds before continuing...")
            sleep(wait_seconds)

        for url in urls:
            response = context.request.get(url, timeout=60000)
            headers = response.headers
            if response.status == 200 and _is_pdf_headers(headers):
                filename = url.split("/")[-1]
                _save_bytes(directory, filename, response.body())
                downloaded_count += 1
                print(f"Downloaded {filename} via playwright-manual")
            else:
                content_type = headers.get("content-type", "unknown")
                print(f"Playwright-manual failed {url}: status={response.status}, content-type={content_type}")
            sleep(sleep_seconds)

        context.close()
        browser.close()

    return downloaded_count


def web_scrape_acea(
    date_range: list[str] = ["2024-01-01", "2025-12-31"],
    directory: str = "PDFs/ACEA",
    method: str = "auto",
    sleep_seconds: float = 2.0,
    headless: bool = True,
    manual_wait_seconds: float = 120.0,
) -> None:
    """
    :param date_range: list containing start and end date for data to be scraped in YYYY-MM-DD format
    :param directory: directory where PDFs will be saved
    :return: nothing. PDF is saved in PDF folder
    """
    if method not in {"auto", "requests", "playwright", "playwright_manual"}:
        raise ValueError("method must be one of: auto, requests, playwright, playwright_manual")

    os.makedirs(directory, exist_ok=True)
    urls = _build_urls(date_range)
    # Example direct link format:
    # https://www.acea.auto/files/Press_release_car_registrations_April_2025.pdf

    total_downloaded = 0

    if method in {"auto", "requests"}:
        failed_urls, downloaded_count = _download_with_requests(urls, directory, sleep_seconds)
        total_downloaded += downloaded_count
    elif method == "playwright_manual":
        failed_urls = []
        downloaded_count = _download_with_playwright_manual(urls, directory, sleep_seconds, manual_wait_seconds)
        total_downloaded += downloaded_count
    else:
        failed_urls = urls

    if method in {"auto", "playwright"} and failed_urls:
        print(f"Trying playwright fallback for {len(failed_urls)} URL(s)...")
        downloaded_count = _download_with_playwright(failed_urls, directory, sleep_seconds, headless)
        total_downloaded += downloaded_count

    print(f"Finished web_scrape_acea. Downloaded {total_downloaded} file(s).")

if __name__ == "__main__":
    web_scrape_acea()