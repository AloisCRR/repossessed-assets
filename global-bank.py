# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "beautifulsoup4==4.14.2",
#     "playwright==1.55.0",
#     "prefect==3.5.0",
#     "requests==2.32.5",
#     "aiohttp==3.13.2",
# ]
# ///

import marimo

__generated_with = "0.17.7"
app = marimo.App(width="columns", app_title="Global Bank Repossessed Assets")

with app.setup:
    import json
    import os
    from typing import List, Set
    from urllib.parse import quote_plus, urlencode

    import aiohttp
    import marimo as mo
    from playwright.async_api import async_playwright
    from prefect import flow, get_run_logger, task


@app.function
@task
async def fetch_all_urls():
    logger = get_run_logger()
    base_url = "https://www.globalbank.com.pa"
    catalog_url = f"{base_url}/bienes-reposeidos/inmueble/catalogo"

    async with async_playwright() as p:
        launch = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                # "--single-process",
            ],
        }

        query = {
            "token": os.environ["BROWSERLESS_TOKEN"],
            "stealth": "true",
            "blockAds": "true",
            "timeout": 600000,
            "launch": json.dumps(launch),
        }

        ws = f"{os.environ['BROWSERLESS_URL']}?{urlencode(query, quote_via=quote_plus)}"

        browser = await p.chromium.connect_over_cdp(ws)

        # Create browser context with route registration
        context = await browser.new_context()

        # Performance optimizations on browser context
        await context.route(
            "**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2,ttf}",
            lambda route: route.abort(),
        )

        try:
            logger.info(f"Fetching URLs from {catalog_url}")

            page = await context.new_page()

            await page.goto(
                catalog_url, timeout=30000, wait_until="domcontentloaded"
            )

            all_links = []

            current_page = 0

            while True:
                # Get property links from current page
                hrefs = await page.locator(
                    'a[href^="/bienes-reposeidos/inmueble/catalogo/"]'
                ).evaluate_all(
                    "links => links.map(link => `https://www.globalbank.com.pa${link.getAttribute('href')}`)"
                )

                all_links.extend(hrefs)

                logger.info(
                    f"Page {current_page + 1}: Found {len(hrefs)} property links"
                )

                # Look for next page button (rel="next")
                next_button = page.locator('a[rel="next"]').first

                count = await next_button.count()

                if count == 0:
                    break  # No more pages

                next_href = await next_button.get_attribute("href")

                if not next_href:
                    break  # No more pages

                # Navigate to next page
                next_url = f"{catalog_url}{next_href}"

                await page.close()

                page = await context.new_page()

                await page.goto(
                    next_url, timeout=30000, wait_until="domcontentloaded"
                )

                current_page += 1

            # Remove duplicates while preserving order
            unique_links = list(dict.fromkeys(all_links))

            logger.info(
                f"Total: Found {len(unique_links)} property links across {current_page + 1} pages"
            )

            return unique_links

        except Exception as e:
            logger.error(f"Error fetching or parsing the catalog page: {e}")
            raise
        finally:
            await context.close()
            await browser.close()


@app.function
@task
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
            f"{directus_url}/items/repossessed_assets_links?filter[company][_eq]=global-bank&limit=-1&fields=link",
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
@task
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
        {"link": link, "is_scraped": False, "company": "global-bank"}
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
@flow
async def global_bank_repossessed_assets_links():
    """Main flow to sync repossessed assets links with Directus."""
    logger = get_run_logger()

    # Get all scraped links
    scraped_links = await fetch_all_urls()
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
    await global_bank_repossessed_assets_links()
    return


if __name__ == "__main__":
    app.run()
