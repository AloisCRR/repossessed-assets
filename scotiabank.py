import marimo

__generated_with = "0.17.7"
app = marimo.App(width="columns", app_title="Scotiabank Repossessed Assets")

with app.setup:
    import re
    from typing import Dict, Optional
    from urllib.parse import urljoin
    import json

    import marimo as mo
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
    description="Fetch all property URLs from Scotiabank repossessed assets catalog.",
    task_run_name="scotiabank-fetch-urls-from-catalog",
)
def fetch_all_urls():
    logger = get_run_logger()
    base_url = "https://pa.scotiabank.com"

    # URLs for different property types
    catalog_urls = [
        f"{base_url}/es/banca-personal/prestamos/propiedades-en-venta/apartamentos-stock.html",
        f"{base_url}/es/banca-personal/prestamos/propiedades-en-venta/casas-y-residencias.html",
        f"{base_url}/es/banca-personal/prestamos/propiedades-en-venta/lotes-y-fincas.html",
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

    try:
        all_links = []

        for catalog_url in catalog_urls:
            logger.info(f"Fetching URLs from {catalog_url}")

            # Fetch the page content
            response = requests.get(catalog_url, headers=headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # Find all property links based on the HTML structure
            property_links = soup.select(
                'a.standalone-link.button_color_blue[href*="/propiedades-en-venta/"]'
            )

            # Extract full URLs
            page_links = []
            for link in property_links:
                href = link.get("href")
                if href:
                    full_url = urljoin(base_url, str(href))
                    page_links.append(full_url)

            all_links.extend(page_links)

            logger.info(f"Found {len(page_links)} property links from {catalog_url}")

        # Remove duplicates while preserving order
        unique_links = list(dict.fromkeys(all_links))

        logger.info(
            f"Total: Found {len(unique_links)} property links across all categories"
        )

        return unique_links

    except Exception as e:
        logger.error(f"Error fetching or parsing catalog pages: {e}")
        raise


@app.function
@task(
    name="Scrape Property Page - Scotiabank",
    description="Scrape individual property page and extract all data.",
    task_run_name="scotiabank-scrape-property-{link_data[id]}",
)
def scrape_property_page_scotiabank(
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

        # Initialize property data with required fields
        property_data = {
            "link_id": link_id,
            "status": "active",
            "price": None,
        }

        # Extract property title from h1
        title_elem = soup.select_one(".bns--title.title--h1 h1")
        title_text = None
        if title_elem:
            title_text = title_elem.get_text(separator=" ", strip=True)

        # Extract property ID from Finca field (for lands)
        property_id = None
        finca_elem = soup.select_one(".sub-heading p")
        if finca_elem:
            property_id = finca_elem.get_text(strip=True)

        if property_id:
            property_data["property_id"] = property_id

        # Extract price from h2
        price_elem = soup.select_one(".bns--title.title--h2 h2")
        if price_elem:
            price_text = price_elem.get_text(strip=True)

            # Look for price patterns like "$192,000.00" or "$227,500"
            price_match = re.search(r"\$[\d,]+\.?\d*", price_text)
            if price_match:
                price_str = price_match.group(0)
                # Remove $ and commas, convert to float
                price_clean = price_str.replace("$", "").replace(",", "")
                try:
                    property_data["price"] = float(price_clean)
                except ValueError:
                    property_data["price"] = None

        # Extract description
        description = None
        desc_elem = soup.find(
            "b", string=re.compile(r"Descripci(ó|o)n\s*:", re.IGNORECASE)
        )
        if desc_elem:
            # Case 1: Description is in the same tag
            desc_text = desc_elem.get_text(strip=True)
            if len(desc_text) > 15:  # "Descripción:" is ~12 chars
                description = desc_text.split(":", 1)[-1].strip()
            else:
                # Case 2: Description is in the next <p> sibling
                p_parent = desc_elem.find_parent("p")
                if p_parent:
                    next_p = p_parent.find_next_sibling("p")
                    if next_p:
                        description = next_p.get_text(strip=True)
        if description:
            property_data["description"] = description

        # Extract address
        address = None
        address_title_elem = soup.find(
            "b", string=re.compile(r"Dirección:", re.IGNORECASE)
        )
        if address_title_elem:
            p_parent = address_title_elem.find_parent("p")
            if p_parent:
                next_p = p_parent.find_next_sibling("p")
                if next_p:
                    address = next_p.get_text(strip=True)

        if not address and title_text:
            address = title_text

        if address:
            property_data["address"] = address[:255]

        # Extract property characteristics
        characteristics_divs = soup.select("._row .col-md-4.col-lg-2")
        for char_div in characteristics_divs:
            p_tag = char_div.select_one(".cmp-text p")
            if p_tag:
                p_text = p_tag.get_text(separator=" ", strip=True)
                value_tag = p_tag.find("b")
                if not value_tag:
                    continue

                value_text = value_tag.get_text(strip=True)

                try:
                    if "Recámaras:" in p_text:
                        property_data["bedrooms"] = int(value_text)
                    elif "Baños:" in p_text:
                        property_data["bathrooms"] = int(value_text)
                    elif "Parqueo:" in p_text:
                        property_data["parking"] = int(value_text)
                    elif "Terreno M" in p_text:
                        property_data["area_m2"] = float(value_text.replace(",", ""))
                    elif "Construcción M" in p_text:
                        property_data["built_area"] = float(value_text.replace(",", ""))
                except (ValueError, TypeError):
                    pass

        # Parse boolean fields from description
        if property_data.get("description"):
            desc_lower = property_data["description"].lower()
            if "sala" in desc_lower:
                property_data["living_room"] = True
            if "comedor" in desc_lower:
                property_data["dining_room"] = True
            if "cocina" in desc_lower:
                property_data["kitchen"] = True
            if "lavandería" in desc_lower:
                property_data["laundry"] = True
            if "terraza" in desc_lower:
                property_data["terrace"] = True
            if "balcón" in desc_lower:
                property_data["balcony"] = True
            if "deposito" in desc_lower:
                property_data["deposit"] = True
            if "cuarto de servicio" in desc_lower:
                property_data["utility_room"] = True
            if "estudio" in desc_lower:
                property_data["studio"] = True

        # Extract images from gallery
        images = []
        gallery_elem = soup.select_one(".bns-image-gallery")
        if gallery_elem:
            json_data = gallery_elem.get("data-bns-json-data")
            if json_data:
                try:
                    data = json.loads(str(json_data))
                    for i, image_info in enumerate(data.get("images", []), 1):
                        image_path = image_info.get("imagePath", "")
                        if image_path:
                            full_url = urljoin("https://pa.scotiabank.com", image_path)
                            images.append(
                                {
                                    "source_url": full_url,
                                    "title": f"Imagen #{i} de bien en venta ubicado en {property_data.get('address', 'N/A')} con el precio {property_data.get('price', 'N/A')}"[
                                        :255
                                    ],
                                }
                            )
                except (json.JSONDecodeError, Exception):
                    pass

        if not images:
            gallery_imgs = soup.select(".image-gallery img.gallery-image-item")
            for i, img in enumerate(gallery_imgs, 1):
                src = img.get("src")
                if src:
                    full_url = urljoin("https://pa.scotiabank.com", src)
                    images.append(
                        {
                            "source_url": full_url,
                            "title": f"Imagen #{i} de bien en venta ubicado en {property_data.get('address', 'N/A')} con el precio {property_data.get('price', 'N/A')}"[
                                :255
                            ],
                        }
                    )

        property_data["images"] = images

        # Determine property type based on URL
        if "apartamentos" in url:
            property_data["property_type"] = "apartamento"
        elif "casas" in url:
            property_data["property_type"] = "casa"
        elif "lotes" in url:
            property_data["property_type"] = "terreno"
        else:
            property_data["property_type"] = "otro"

        # Store additional attributes
        additional_attrs = {}
        if title_text:
            additional_attrs["title"] = title_text

        rate_elem = soup.select_one(".bns--title.title--h3 h3")
        if rate_elem:
            rate_text = rate_elem.get_text(strip=True)
            if "Tasa desde:" in rate_text:
                additional_attrs["financing_rate"] = rate_text

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
    scrape_property_page_scotiabank(
        {
            "link": "https://pa.scotiabank.com/es/banca-personal/prestamos/propiedades-en-venta/lotes-y-fincas/canyon-village-montanas-de-caldera001.html",
            "id": "1a44c78d-3a3f-4814-911e-bd920bf91126",
        }
    )
    return


@app.function
def generate_flow_run_name():
    date = datetime.datetime.now(datetime.timezone.utc)
    return f"scotiabank_repossessed_assets_{date:%Y-%m-%d_%H-%M-%S}"


@app.function
@flow(
    description="Scrape Scotiabank repossessed assets",
    flow_run_name=generate_flow_run_name,
)
async def scotiabank_repossessed_assets():
    """Main flow to sync Scotiabank repossessed assets links with Directus and scrape property data."""
    logger = get_run_logger()

    # Get all scraped links from Scotiabank
    scraped_links = fetch_all_urls()
    scraped_links_set = set(scraped_links)

    # Get existing links from Directus
    existing_links = await get_existing_links_from_directus("scotiabank")

    # Compare and find differences
    new_links = list(scraped_links_set - existing_links)

    logger.info(f"Found {len(new_links)} new links to add for Scotiabank")

    # Sync with Directus
    await add_new_links_to_directus(new_links, "scotiabank")

    logger.info("Link sync completed successfully")

    # Get unscraped links
    unscraped_links = await get_unscraped_links_from_directus("scotiabank")

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

        scraping_futures = scrape_property_page_scotiabank.map(batch)

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
    await scotiabank_repossessed_assets()
    return


if __name__ == "__main__":
    app.run()
