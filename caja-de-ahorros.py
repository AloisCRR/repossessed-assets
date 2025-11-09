# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "beautifulsoup4==4.14.2",
#     "prefect==3.5.0",
#     "requests==2.32.5",
#     "aiohttp==3.13.2",
# ]
# ///

import marimo

__generated_with = "0.17.7"
app = marimo.App(
    width="columns",
    app_title="Caja de Ahorros Repossessed Assets",
)

with app.setup:
    import json
    import os
    import re
    from typing import List, Set
    from urllib.parse import quote_plus, urlencode

    import aiohttp
    import requests
    from bs4 import BeautifulSoup
    from prefect import flow, get_run_logger, task

    import datetime


@app.function
@task(
    name="Fetch All URLs",
    description="Fetch all property URLs from Caja de Ahorros repossessed assets catalog.",
    task_run_name="caja-de-ahorros-fetch-urls-from-catalog",
)
def fetch_all_urls():
    logger = get_run_logger()
    catalog_url = (
        "https://www.cajadeahorros.com.pa/propiedades/bienes-reposeidos/"
    )

    cookies = {
        "PORTAL-XSESSIONID": "1762648165.101.2206.779811|29e68a15732949f1942f74c137980c8c",
        "pum-50286": "true",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en,es-ES;q=0.5",
        # 'Accept-Encoding': 'gzip, deflate, br, zstd',
        "DNT": "1",
        "Sec-GPC": "1",
        "Connection": "keep-alive",
        # 'Cookie': 'PORTAL-XSESSIONID=1762648165.101.2206.779811|29e68a15732949f1942f74c137980c8c; pum-50286=true',
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Priority": "u=0, i",
    }

    try:
        logger.info(f"Fetching URLs from {catalog_url}")

        response = requests.get(
            catalog_url, cookies=cookies, headers=headers, timeout=30
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Find all script tags
        script_tags = soup.find_all("script")

        all_properties_data = None

        for script in script_tags:
            if script.string and "var allProperties" in script.string:
                # Extract the JSON data from the script
                match = re.search(
                    r"var allProperties = (\[.*?\]);", script.string, re.DOTALL
                )

                if match:
                    try:
                        all_properties_data = json.loads(match.group(1))
                        break
                    except json.JSONDecodeError as e:
                        logger.error(
                            f"Failed to parse allProperties JSON: {e}"
                        )
                        continue

        if not all_properties_data:
            logger.error(
                "Could not find allProperties variable in any script tag"
            )
            raise Exception("allProperties variable not found")

        # Extract URLs from the properties data
        property_links = []
        for property_item in all_properties_data:
            if "url" in property_item:
                property_links.append(property_item["url"])

        # Remove duplicates while preserving order
        unique_links = list(dict.fromkeys(property_links))

        logger.info(f"Found {len(unique_links)} property links")

        return unique_links

    except Exception as e:
        logger.error(f"Error fetching or parsing the catalog page: {e}")
        raise


@app.function
@task(
    name="Get Existing Links from Directus",
    description="Get all existing links from Directus repossessed_assets_links collection.",
    task_run_name="caja-de-ahorros-get-existing-links-directus",
)
async def get_existing_links_from_directus() -> Set[str]:
    """Get all existing links from Directus repossessed_assets_links collection."""
    logger = get_run_logger()
    directus_url = os.environ["DIRECTUS_URL"]
    directus_token = os.environ["DIRECTUS_TOKEN"]

    headers = {
        "Authorization": f"Bearer {directus_token}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        # Get all items from the collection
        async with session.get(
            f"{directus_url}/items/repossessed_assets_links?filter[company][_eq]=caja-de-ahorros&limit=-1&fields=link",
            headers=headers,
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(
                    f"Failed to fetch existing links: {response.status} - {error_text}"
                )
                raise Exception(
                    f"Directus API error: {response.status} - {error_text}"
                )

            data = await response.json()
            existing_links = {item["link"] for item in data["data"]}
            logger.info(
                f"Found {len(existing_links)} existing links in Directus"
            )
            return existing_links


@app.function
@task(
    name="Add New Links to Directus",
    description="Add new links to Directus repossessed_assets_links collection.",
    task_run_name="caja-de-ahorros-add-new-links-to-directus",
)
async def add_new_links_to_directus(new_links: List[str]) -> None:
    """Add new links to Directus repossessed_assets_links collection."""
    logger = get_run_logger()
    directus_url = os.environ["DIRECTUS_URL"]
    directus_token = os.environ["DIRECTUS_TOKEN"]

    headers = {
        "Authorization": f"Bearer {directus_token}",
        "Content-Type": "application/json",
    }

    if not new_links:
        logger.info("No new links to add")
        return

    # Prepare batch insert data
    items = [
        {"link": link, "is_scraped": False, "company": "caja-de-ahorros"}
        for link in new_links
    ]

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{directus_url}/items/repossessed_assets_links",
            headers=headers,
            json=items,
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(
                    f"Failed to add new links: {response.status} - {error_text}"
                )
                raise Exception(
                    f"Directus API error: {response.status} - {error_text}"
                )

            result = await response.json()
            logger.info(f"Added {len(result['data'])} new links to Directus")


@app.function
def generate_flow_run_name():
    date = datetime.datetime.now(datetime.timezone.utc)
    return f"caja_de_ahorros_repossessed_assets_{date:%Y-%m-%d_%H-%M-%S}"


@app.function
@flow(
    description="Scrape Caja de Ahorros repossessed assets",
    flow_run_name=generate_flow_run_name,
)
async def caja_de_ahorros_repossessed_assets():
    """Main flow to sync repossessed assets links with Directus."""
    logger = get_run_logger()

    # Get all scraped links
    scraped_links = fetch_all_urls()
    scraped_links_set = set(scraped_links)

    # Get existing links from Directus
    existing_links = await get_existing_links_from_directus()

    # Compare and find differences
    new_links = list(scraped_links_set - existing_links)

    logger.info(f"Found {len(new_links)} new links to add")

    # Sync with Directus
    await add_new_links_to_directus(new_links)

    logger.info("Sync completed successfully")


@app.cell
async def _():
    await caja_de_ahorros_repossessed_assets()
    return


if __name__ == "__main__":
    app.run()
