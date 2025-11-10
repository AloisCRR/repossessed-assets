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
    import re
    from typing import Dict, Optional
    from urllib.parse import quote_plus, urlencode, urljoin

    import marimo as mo
    from playwright.async_api import async_playwright
    from prefect import flow, get_run_logger, task
    from prefect.futures import wait
    import requests
    from bs4 import BeautifulSoup

    import datetime
    from directus_tasks import (
        get_existing_links_from_directus,
        add_new_links_to_directus,
        get_unscraped_links_from_directus,
        save_property_data,
        mark_link_as_scraped,
    )


@app.function
@task(
    name="Fetch All URLs",
    description="Fetch all property URLs from Global Bank repossessed assets catalog.",
    task_run_name="global-bank-fetch-urls-from-catalog",
)
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

            await page.goto(catalog_url, timeout=30000, wait_until="domcontentloaded")

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

                await page.goto(next_url, timeout=30000, wait_until="domcontentloaded")

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
@task(
    name="Scrape Property Page",
    description="Scrape individual property page and extract all data.",
    task_run_name="global-bank-scrape-property-{link_data[id]}",
)
def scrape_property_page(link_data: Dict[str, str]) -> Optional[Dict]:
    """Scrape individual property page and extract all data."""
    logger = get_run_logger()
    link_id = link_data["id"]
    url = link_data["link"]

    cookies = {
        "visid_incap_723201": "/yxCCbaaQgKJFARcRyU+Az7rC2kAAAAAQUIPAAAAAACNVc5ZE2zEsVDzfa1ta+X2",
        "visid_incap_2602219": "DmTOBavEQZ60fIeG2PMPT93cI2gAAAAAQUIPAAAAAAD3XuGX+Obb27cAoIG3tYFN",
        "visid_incap_723182": "HxAZAXPtRBKsUvA32ULcZNThI2gAAAAAQUIPAAAAAAAvKSeKbTxud9VgzGdhAO+Y",
        "visid_incap_2671521": "glfeoK8bQ8S6VPnRxOIJGWpj/WgAAAAAQUIPAAAAAABog5vf57MAbFbRYtUE70VK",
        "nlbi_2671521": "wJCSP1vLpn0iRwhRC3IBEgAAAADXlgaAD/TY/rrJf3YLItY0",
        "incap_ses_995_723201": "ZM1NLcHOxRkM6UQRTfPODa9+DmkAAAAAK+jMz+jPI1rRmO3b6QijFw==",
        "incap_ses_1841_723201": "isgIDWXmix5Txus4TYyMGW8rDGkAAAAAfTN28SM3vqh+CEB/toaZVw==",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en,es-ES;q=0.5",
        "DNT": "1",
        "Sec-GPC": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "If-Modified-Since": "Fri, 07 Nov 2025 23:20:14 GMT",
        "If-None-Match": '"1762557614-gzip"',
        "Priority": "u=0, i",
    }

    try:
        logger.info(f"Scraping property page: {url}")

        # Fetch the page content
        response = requests.get(url, cookies=cookies, headers=headers, timeout=120)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Extract property data
        property_data = {"link_id": link_id, "status": "active", "price": 0}

        # Extract property ID
        property_id_elem = soup.select_one(
            "div.field--name-field-identificador-de-finca .field__item"
        )

        if property_id_elem:
            property_data["property_id"] = property_id_elem.get_text(strip=True)

        # Extract property type
        property_type_elem = soup.select_one(
            "div.field--name-field-tipo-de-propiedad .field__item"
        )

        if property_type_elem:
            property_data["property_type"] = property_type_elem.get_text(strip=True)

        # Extract address
        address_elem = soup.select_one(
            "div.field--name-field-direccion-completa .field__item"
        )

        if address_elem:
            property_data["address"] = address_elem.get_text(strip=True)

        # Extract and parse price
        price_elem = soup.select_one(
            "div.field--name-field-precio-de-venta .field__item"
        )

        if price_elem:
            price_text = price_elem.get_text(strip=True)
            # Remove all characters except digits and dots
            price_clean = re.sub(r"[^\d.]", "", price_text)

            try:
                property_data["price"] = float(price_clean) if price_clean else None
            except ValueError:
                property_data["price"] = None

        # Extract area measurements
        area_elem = soup.select_one(
            "div.field--name-field-metros-del-terreno .field__item"
        )

        if area_elem:
            try:
                property_data["area_m2"] = float(area_elem.get_text(strip=True))
            except ValueError:
                property_data["area_m2"] = None

        built_area_elem = soup.select_one(
            "div.field--name-field-metros-de-construccion .field__item"
        )
        if built_area_elem:
            try:
                property_data["built_area"] = float(
                    built_area_elem.get_text(strip=True)
                )
            except ValueError:
                property_data["built_area"] = None

        # Extract hectares
        hectares_elem = soup.select_one("div.field--name-field-hectareas .field__item")
        if hectares_elem:
            try:
                property_data["hectares"] = int(hectares_elem.get_text(strip=True))
            except ValueError:
                property_data["hectares"] = None

        # Extract room counts
        bedrooms_elem = soup.select_one("div.field--name-field-recamaras .field__item")
        if bedrooms_elem:
            try:
                property_data["bedrooms"] = int(bedrooms_elem.get_text(strip=True))
            except ValueError:
                property_data["bedrooms"] = None

        bathrooms_elem = soup.select_one("div.field--name-field-banios .field__item")
        if bathrooms_elem:
            try:
                property_data["bathrooms"] = int(bathrooms_elem.get_text(strip=True))
            except ValueError:
                property_data["bathrooms"] = None

        # Extract boolean features
        living_room_elem = soup.select_one("div.field--name-field-sala .field__item")
        property_data["living_room"] = bool(
            living_room_elem and living_room_elem.get_text(strip=True)
        )

        dining_room_elem = soup.select_one("div.field--name-field-comedor .field__item")
        property_data["dining_room"] = bool(
            dining_room_elem and dining_room_elem.get_text(strip=True)
        )

        kitchen_elem = soup.select_one("div.field--name-field-cocina .field__item")
        property_data["kitchen"] = bool(
            kitchen_elem and kitchen_elem.get_text(strip=True)
        )

        laundry_elem = soup.select_one("div.field--name-field-lavanderia .field__item")
        property_data["laundry"] = bool(
            laundry_elem and laundry_elem.get_text(strip=True)
        )

        parking_elem = soup.select_one(
            "div.field--name-field-estacionamiento .field__item"
        )
        if parking_elem:
            try:
                property_data["parking"] = int(parking_elem.get_text(strip=True))
            except ValueError:
                property_data["parking"] = None

        # Extract coordinates from Drupal settings JSON
        script_elem = soup.select_one(
            'script[type="application/json"][data-drupal-selector="drupal-settings-json"]'
        )

        if script_elem:
            try:
                drupal_settings = json.loads(script_elem.string or "")
                geofield_maps = drupal_settings.get("geofield_google_map", {})
                if geofield_maps:
                    first_map_key = next(iter(geofield_maps))
                    coordinates = (
                        geofield_maps[first_map_key]
                        .get("data", {})
                        .get("features", [{}])[0]
                        .get("geometry", {})
                        .get("coordinates")
                    )
                    if coordinates and len(coordinates) == 2:
                        lon, lat = coordinates[0], coordinates[1]
                        property_data["geog"] = {
                            "type": "Point",
                            "coordinates": [lon, lat],
                        }
                        property_data["latitude"] = str(lat)
                        property_data["longitude"] = str(lon)
            except (
                json.JSONDecodeError,
                AttributeError,
                KeyError,
                IndexError,
                TypeError,
            ):
                logger.warning(f"Could not extract coordinates from {url}")

        # Extract images
        images = []

        img_elems = soup.select("div.field--name-field-imagenes-del-inmueble img")

        base_url = "https://www.globalbank.com.pa"

        image_count = 0

        for img in img_elems:
            src = img.get("src")
            if src:
                full_url = urljoin(base_url, str(src))

                image_count += 1

                images.append(
                    {
                        "source_url": full_url,
                        "title": f"Imagen #{image_count} de bien en venta ubicado en {property_data.get('address', 'N/A')} con el precio {property_data.get('price', 'N/A')}",
                    }
                )

        property_data["images"] = images

        # Store additional raw attributes
        additional_attrs = {}
        all_field_items = soup.select("div.inmueble-atributos .field")
        for field in all_field_items:
            label_elem = field.select_one(".field__label")
            value_elem = field.select_one(".field__item")
            if label_elem and value_elem:
                label = label_elem.get_text(strip=True).replace(":", "")
                value = value_elem.get_text(strip=True)
                additional_attrs[label] = value

        property_data["additional_attrs"] = additional_attrs

        logger.info(
            f"Successfully scraped property {property_data.get('property_id', 'unknown')}"
        )
        return property_data

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None


@app.function
def generate_flow_run_name():
    date = datetime.datetime.now(datetime.timezone.utc)
    return f"global_bank_repossessed_assets_{date:%Y-%m-%d_%H-%M-%S}"


@app.function
@flow(
    description="Scrape Global Bank repossessed assets",
    flow_run_name=generate_flow_run_name,
)
async def global_bank_repossessed_assets():
    """Main flow to sync repossessed assets links with Directus."""
    logger = get_run_logger()

    # Get all scraped links
    scraped_links = await fetch_all_urls()
    scraped_links_set = set(scraped_links)

    # Get existing links from Directus
    existing_links = await get_existing_links_from_directus("global-bank")

    # Compare and find differences
    new_links = list(scraped_links_set - existing_links)

    logger.info(f"Found {len(new_links)} new links to add")

    # Sync with Directus
    await add_new_links_to_directus(new_links, "global-bank")

    logger.info("Sync completed successfully")

    # Get unscraped links
    unscraped_links = await get_unscraped_links_from_directus("global-bank")

    if not unscraped_links:
        logger.info("No unscraped links found")
        return

    logger.info(f"Found {len(unscraped_links)} unscraped links to process")

    # Process links in batches of 5
    batch_size = 5
    total_processed = 0

    for i in range(0, len(unscraped_links), batch_size):
        batch = unscraped_links[i : i + batch_size]

        logger.info(f"Processing batch {i // batch_size + 1}: {len(batch)} links")

        scraping_futures = scrape_property_page.map(batch)

        # Wait for all scraping tasks to complete and handle failures
        done, not_done = wait(scraping_futures)

        # Process successful results
        for future in done:
            if future.state.is_completed():
                try:
                    property_data = future.result()
                    if property_data:
                        # Extract link_id from the response data
                        link_id = property_data["link_id"]

                        # Save property data
                        save_success = await save_property_data(property_data)

                        if save_success:
                            # Mark link as scraped
                            await mark_link_as_scraped(link_id)

                            total_processed += 1
                        else:
                            logger.warning(f"Failed to save data for link {link_id}")
                    else:
                        logger.warning("No data scraped from successful task")
                except Exception as e:
                    logger.error(f"Error processing successful task result: {e}")
            else:
                # Handle failed tasks
                logger.error(f"Task failed: {future.state}")

        # Handle any tasks that didn't complete
        if not_done:
            logger.warning(f"{len(not_done)} tasks did not complete")

    logger.info(
        f"Scraping completed. Total processed: {total_processed}/{len(unscraped_links)} - {round((total_processed / len(unscraped_links)) * 100, 2)}%"
    )


@app.cell
async def _():
    await global_bank_repossessed_assets()
    return


if __name__ == "__main__":
    app.run()
