import marimo

__generated_with = "0.17.7"
app = marimo.App(width="columns", app_title="Banco General Repossessed Assets")

with app.setup:
    import datetime
    import re
    import time
    from typing import Dict, Optional

    import marimo as mo
    import requests
    from bs4 import BeautifulSoup
    from prefect import flow, get_run_logger, task
    from prefect.futures import wait
    from unidecode import unidecode

    from directus_tasks import (
        add_new_links_to_directus,
        get_existing_links_from_directus,
        get_unscraped_links_from_directus,
        mark_link_as_scraped,
        save_property_data,
    )


@app.function
@task(
    name="Fetch All Banco General URLs",
    description="Fetch all property URLs from Banco General repossessed assets catalog.",
    task_run_name="banco-general-fetch-urls-from-catalog",
)
async def fetch_all_banco_general_urls():
    """Fetch all property URLs from Banco General repossessed assets catalog."""
    logger = get_run_logger()

    # Base URLs for housing and commercial properties
    base_urls = [
        "https://www.bgeneral.com/clasificados-bg/viviendas-reposeidas/",
        "https://www.bgeneral.com/clasificados-bg/comerciales/",
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en,es-ES;q=0.5",
        "DNT": "1",
        "Sec-GPC": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    all_links = []

    for base_url in base_urls:
        logger.info(f"Starting to scrape: {base_url}")

        page_num = 1
        page_links = []

        while True:
            # Construct page URL
            if page_num == 1:
                page_url = base_url
            else:
                page_url = f"{base_url}page/{page_num}/"

            try:
                logger.info(f"Fetching page {page_num}: {page_url}")

                # Add delay between requests
                if page_num > 1:
                    time.sleep(2)

                response = requests.get(page_url, headers=headers, timeout=30)
                response.raise_for_status()

                soup = BeautifulSoup(response.content, "html.parser")

                # Check if no properties found
                no_properties = soup.select_one(".searched-properties")
                if (
                    no_properties
                    and "No se encontraron propiedades" in no_properties.get_text()
                ):
                    logger.info(f"No more properties found on page {page_num}")
                    break

                # Find property links
                property_links = []

                # Look for links within property cards
                property_cards = soup.select(".propery-style-6 a[target='_blank']")
                for link_elem in property_cards:
                    href = str(link_elem.get("href", ""))
                    if href and href.startswith("https://www.bgeneral.com/property/"):
                        property_links.append(href)

                # Alternative selector if the above doesn't work
                if not property_links:
                    link_elems = soup.select("a[href*='/property/']")
                    for link_elem in link_elems:
                        href = link_elem.get("href")
                        if (
                            href
                            and isinstance(href, str)
                            and href.startswith("https://www.bgeneral.com/property/")
                        ):
                            property_links.append(href)

                # Alternative selector if the above doesn't work
                if not property_links:
                    link_elems = soup.select("a[href*='/property/']")
                    for link_elem in link_elems:
                        href = link_elem.get("href")
                        if href:
                            href_str = str(href)
                            if href_str.startswith(
                                "https://www.bgeneral.com/property/"
                            ):
                                property_links.append(href_str)

                if not property_links:
                    logger.warning(f"No property links found on page {page_num}")
                    break

                page_links.extend(property_links)
                logger.info(
                    f"Page {page_num}: Found {len(property_links)} property links"
                )

                page_num += 1

            except requests.RequestException as e:
                logger.error(f"Error fetching page {page_url}: {e}")
                # Continue with next page instead of failing entirely
                page_num += 1
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing page {page_url}: {e}")
                page_num += 1
                continue

        # Remove duplicates while preserving order
        unique_page_links = list(dict.fromkeys(page_links))
        all_links.extend(unique_page_links)

        logger.info(
            f"Completed {base_url}: Found {len(unique_page_links)} unique links"
        )

    # Remove duplicates across both URLs while preserving order
    unique_all_links = list(dict.fromkeys(all_links))

    logger.info(f"Total: Found {len(unique_all_links)} unique property links")

    return unique_all_links


@app.function
@task(
    name="Scrape Property Page - Banco General",
    description="Scrape individual property page and extract all data.",
    task_run_name="banco-general-scrape-property-{link_data[id]}",
)
def scrape_property_page_banco_general(
    link_data: Dict[str, str],
) -> Optional[Dict]:
    """Scrape individual property page and extract all data."""
    logger = get_run_logger()
    link_id = link_data["id"]
    url = link_data["link"]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en,es-ES;q=0.5",
        "DNT": "1",
        "Sec-GPC": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    try:
        logger.info(f"Scraping property page: {url}")

        # Fetch the page content
        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Extract property data
        property_data = {"link_id": link_id, "status": "active", "price": 0}

        # Extract property title
        title_elem = soup.select_one(".fusion-page-title-captions h1.entry-title")
        if title_elem:
            property_data["title"] = title_elem.get_text(strip=True)

        # Extract price
        price_elem = soup.select_one(".large-price .rem-price-amount")

        if price_elem:
            price_text = price_elem.get_text(strip=True)

            price_clean = re.sub(
                r"[^\d.]",
                "",
                price_text,
            )
            try:
                property_data["price"] = float(price_clean)
                property_data["currency"] = "PAB"  # Panamanian Balboa
            except ValueError:
                property_data["price"] = None

        # Extract property information from details section
        details = {}
        detail_rows = soup.select(".details.tab-general_settings .row .detail")
        for detail in detail_rows:
            title_elem = detail.select_one(".rem-single-field-title")
            value_elem = detail.select_one(".rem-single-field-value")
            if title_elem and value_elem:
                title = title_elem.get_text(strip=True).replace(":", "")
                value = value_elem.get_text(strip=True)
                details[title] = value

        # Map specific fields
        if "Finca #" in details:
            property_data["property_id"] = details["Finca #"]

        if "Tipo de propiedad" in details:
            property_data["property_type"] = details["Tipo de propiedad"]

        if "Ubicación" in details:
            property_data["address"] = details["Ubicación"]

        if "Dirección" in details:
            property_data["address"] = details["Dirección"]

        if "Área de construcción" in details:
            unit_clean = (
                details["Área de construcción"]
                .lower()
                .replace("m²", "")
                .replace("m2", "")
                .replace("sqm", "")
                .replace("m", "")
                .strip()
            )  # Remove commas (thousands separators)

            if "," in unit_clean:
                # Check if comma is used as decimal separator (followed by 1-3 digits)
                if re.search(r",\d{1,3}$", unit_clean):
                    unit_clean = unit_clean.replace(
                        ",", ".", 1
                    )  # Replace only the first comma
                else:
                    # If comma is not followed by digits, remove it (probably a thousand separator)
                    unit_clean = unit_clean.replace(",", "")

            built_area_clean = re.sub(
                r"[^\d.]",
                "",
                unit_clean,
            )

            try:
                property_data["built_area"] = float(built_area_clean)
            except ValueError:
                pass

        if "Área de terreno" in details:
            unit_clean = (
                details["Área de terreno"]
                .lower()
                .replace("m²", "")
                .replace("m2", "")
                .replace("sqm", "")
                .replace("m", "")
                .strip()
            )  # Remove commas (thousands separators)

            if "," in unit_clean:
                # Check if comma is used as decimal separator (followed by 1-3 digits)
                if re.search(r",\d{1,3}$", unit_clean):
                    unit_clean = unit_clean.replace(
                        ",", ".", 1
                    )  # Replace only the first comma
                else:
                    # If comma is not followed by digits, remove it (probably a thousand separator)
                    unit_clean = unit_clean.replace(",", "")

            area_m2_clean = re.sub(
                r"[^\d.]",
                "",
                unit_clean,
            )

            try:
                property_data["area_m2"] = float(area_m2_clean)
            except ValueError:
                pass

        if "Habitaciones" in details:
            try:
                property_data["bedrooms"] = int(details["Habitaciones"])
            except ValueError:
                pass

        if "Baños" in details:
            try:
                property_data["bathrooms"] = int(details["Baños"])
            except ValueError:
                pass

        if "Estacionamientos" in details:
            try:
                parking_value = float(
                    details["Estacionamientos"]
                )  # Convert to float first
                property_data["parking"] = int(round(parking_value))
            except (ValueError, TypeError):
                pass  # Handles conversion failures (e.g., non-numeric string like "N/A")

        # Extract coordinates from latitude/longitude section
        lat_elem = soup.select_one(".wrap_property_latitude .rem-single-field-value")
        lon_elem = soup.select_one(".wrap_property_longitude .rem-single-field-value")

        if lat_elem and lon_elem:
            try:
                lat = float(lat_elem.get_text(strip=True))
                lon = float(lon_elem.get_text(strip=True))
                property_data["latitude"] = str(lat)
                property_data["longitude"] = str(lon)
                property_data["geog"] = {
                    "type": "Point",
                    "coordinates": [lon, lat],
                }
            except ValueError:
                pass

        # Extract property features (boolean fields)
        features = soup.select(".details.tab-property_details span.detail")

        feature_mapping = {
            "living_room": "Sala-comedor",
            "dining_room": "Sala-comedor",
            "kitchen": "Cocina",
            "laundry": "Lavandería",
            "social_area": "Área social",
            "security": "Seguridad 24 horas",
            "balcony": "Balcón",
            "elevator": "Elevadores",
            "swimming_pool": "Piscina",
            "terrace": "Terraza",
            "studio": "Estudio",
            "deposit": "Depósito",
            "utility_room": "Cuarto de Servicio",
        }

        for feature in features:
            feature_text = unidecode(feature.get_text(strip=True).lower())

            for field_name, feature_name in feature_mapping.items():
                feature_name_normalized = unidecode(feature_name).lower()

                if feature_name_normalized in feature_text:
                    property_data[field_name] = True

        # Extract images
        images = []
        img_elems = soup.select(".fotorama-custom img.skip-lazy.rem-slider-image")

        for i, img in enumerate(img_elems, 1):
            src = img.get("src")
            if src:
                images.append(
                    {
                        "source_url": src,
                        "title": f"Imagen #{i} de bien en venta ubicado en {property_data.get('address', 'N/A')} con el precio {property_data.get('price', 'N/A')}",
                    }
                )

        property_data["images"] = images

        # Store additional raw attributes
        additional_attrs = {}
        for title, value in details.items():
            # if title not in [
            #     "Tipo de propiedad",
            #     "Ubicación",
            #     "Dirección",
            #     "Área de construcción",
            #     "Habitaciones",
            #     "Baños",
            #     "Estacionamientos",
            # ]:
            additional_attrs[title] = value

        for feature in features:
            feature_text = feature.get_text(strip=True)
            if feature_text:
                additional_attrs[feature_text] = "true"

        property_data["additional_attrs"] = additional_attrs

        logger.info(
            f"Successfully scraped property {property_data.get('property_id', 'unknown')}"
        )
        return property_data

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None


@app.cell
def _():
    scrape_property_page_banco_general(
        {
            "link": "https://www.bgeneral.com/property/parque-lefevre-casa-52-21/",
            "id": "ca868094-85e6-4026-9631-f01c4f3ba355",
        }
    )
    return


@app.function
def generate_flow_run_name_banco_general():
    date = datetime.datetime.now(datetime.timezone.utc)
    return f"banco_general_repossessed_assets_{date:%Y-%m-%d_%H-%M-%S}"


@app.function
@flow(
    description="Scrape Banco General repossessed assets",
    flow_run_name=generate_flow_run_name_banco_general,
)
async def banco_general_repossessed_assets():
    """Main flow to sync Banco General repossessed assets links with Directus and scrape property data."""
    logger = get_run_logger()

    # Get all scraped links from Banco General
    scraped_links = await fetch_all_banco_general_urls()
    scraped_links_set = set(scraped_links)

    # Get existing links from Directus
    existing_links = await get_existing_links_from_directus("banco-general")

    # Compare and find differences
    new_links = list(scraped_links_set - existing_links)

    logger.info(f"Found {len(new_links)} new links to add for Banco General")

    # Sync with Directus
    await add_new_links_to_directus(new_links, "banco-general")

    logger.info("Link sync completed successfully")

    # Get unscraped links
    unscraped_links = await get_unscraped_links_from_directus("banco-general")

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

        scraping_futures = scrape_property_page_banco_general.map(batch)

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
    await banco_general_repossessed_assets()
    return


if __name__ == "__main__":
    app.run()
