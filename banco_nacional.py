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
app = marimo.App(
    width="columns",
    app_title="Banco Nacional Repossessed Assets",
)

with app.setup:
    import re
    from typing import Dict, Optional
    from urllib.parse import urljoin

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
    description="Fetch all property URLs from Banco Nacional repossessed assets catalog.",
    task_run_name="banco-nacional-fetch-urls-from-catalog",
)
def fetch_all_urls():
    logger = get_run_logger()
    base_url = "https://www.banconal.com.pa"
    catalog_url = f"{base_url}/bienes/bienes-adquiridos/"

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
        logger.info(f"Fetching URLs from {catalog_url}")

        all_links = []
        current_page = 1

        while True:
            # Construct URL for current page
            if current_page == 1:
                page_url = catalog_url
            else:
                page_url = f"{catalog_url}?product-page={current_page}"

            logger.info(f"Fetching page {current_page}: {page_url}")

            # Fetch the page content
            response = requests.get(page_url, headers=headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # Find all product links
            product_links = soup.select(
                'a.woocommerce-LoopProduct-link[href^="https://www.banconal.com.pa/product/"]'
            )

            # Extract full URLs
            page_links = []
            for link in product_links:
                href = link.get("href")
                if href:
                    full_url = urljoin(base_url, str(href))
                    page_links.append(full_url)

            all_links.extend(page_links)

            logger.info(f"Page {current_page}: Found {len(page_links)} property links")

            if len(page_links) == 0:
                break  # No more property links on this page

            current_page += 1

        # Remove duplicates while preserving order
        unique_links = list(dict.fromkeys(all_links))

        logger.info(
            f"Total: Found {len(unique_links)} property links across {current_page - 1} pages"
        )

        return unique_links

    except Exception as e:
        logger.error(f"Error fetching or parsing the catalog page: {e}")
        raise


@app.function
@task(
    name="Scrape Property Page - Banco Nacional",
    description="Scrape individual property page and extract all data.",
    task_run_name="banco-nacional-scrape-property-{link_data[id]}",
)
def scrape_property_page_banco_nacional(
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
            "currency": "PAB",
        }

        # Extract property title
        title_elem = soup.select_one(".product_title.entry-title")
        title_text = None
        if title_elem:
            title_text = title_elem.get_text(strip=True)

        # Extract property ID from Finca field in attributes table, fallback to SKU
        property_id = None

        # First try to get from Finca field in attributes table
        finca_row = soup.select_one(
            "tr.woocommerce-product-attributes-item--attribute_pa_finca"
        )
        if finca_row:
            finca_value = finca_row.select_one(
                "td.woocommerce-product-attributes-item__value p"
            )
            if finca_value:
                property_id = finca_value.get_text(strip=True)

        # Fallback to SKU if Finca not found
        if not property_id:
            sku_elem = soup.select_one(".sku_wrapper .sku")
            if sku_elem:
                property_id = sku_elem.get_text(strip=True)

        if property_id:
            property_data["property_id"] = property_id

        # Extract price
        price_elem = soup.select_one(".summary .price .woocommerce-Price-amount.amount")

        if price_elem:
            price_text = price_elem.get_text(strip=True)

            # Step 1: Remove known currency prefixes (adjust the pattern if needed for other currencies)
            # This targets common prefixes like "B/." , "$", "€", etc., followed by the number
            # We use regex to remove everything before the first digit
            # Alternative: Specific removal for your case
            price_text = re.sub(
                r"^[^0-9]*", "", price_text
            )  # Remove everything before the first digit

            # Or, more specific to your currency: Remove "B/." prefix exactly
            # price_text = price_text.replace("B/.", "").strip()

            # Step 2: Remove non-numeric except digits, comma (thousands), and period (decimal)
            price_clean = re.sub(r"[^\d.,]", "", price_text)

            # Step 3: Remove thousands separators (commas) - keep decimal periods
            price_clean = price_clean.replace(",", "")

            try:
                property_data["price"] = float(price_clean) if price_clean else None
            except ValueError:
                property_data["price"] = None

        # Extract short description for address and room counts
        desc_elem = soup.select_one(".woocommerce-product-details__short-description p")

        # Extract additional description from accordion if exists
        additional_description = ""

        desc_accordion = soup.select_one("#accordion-description")

        if desc_accordion:
            # Try to extract from list items first (the structure you provided)
            list_items = desc_accordion.select("ul.list-group li.list-group-item")

            if list_items:
                additional_description = " ".join(
                    [item.get_text(strip=True) for item in list_items]
                )
            else:
                # Fallback to paragraphs
                desc_paragraphs = desc_accordion.select("p")
                if desc_paragraphs:
                    additional_description = " ".join(
                        [p.get_text(strip=True) for p in desc_paragraphs]
                    )

        if desc_elem:
            desc_text = desc_elem.get_text(strip=True)

            property_data["description"] = desc_text

            # Parse address from description
            # Look for location patterns like "Ubicación: ..." or extract first part before parentheses
            if "Ubicación:" in desc_text:
                address_part = desc_text.split("Ubicación:")[1].split("(")[0].strip()
                property_data["address"] = address_part
            else:
                # Extract text before parentheses as address
                address_part = desc_text.split("(")[0].strip()
                property_data["address"] = address_part if address_part else None

            # Parse room counts from patterns like "(2R, 1B)"
            room_match = re.search(r"\((\d+)R,\s*(\d+)B\)", desc_text)
            if room_match:
                try:
                    property_data["bedrooms"] = int(room_match.group(1))
                    property_data["bathrooms"] = int(room_match.group(2))
                except ValueError:
                    pass

        # Fallback: try to extract address from additional_description if not found yet
        if not property_data.get("address") and additional_description:
            if "Ubicación:" in additional_description:
                address_part = (
                    additional_description.split("Ubicación:")[1].split(".")[0].strip()
                )
                property_data["address"] = address_part

        # Extract property details from the attributes table
        details = {}
        detail_rows = soup.select(".woocommerce-product-attributes.shop_attributes tr")

        for row in detail_rows:
            label_elem = row.select_one("th")
            value_elem = row.select_one("td p")
            if label_elem and value_elem:
                label = label_elem.get_text(strip=True).lower()
                value = value_elem.get_text(strip=True)
                details[label] = value

                # Map specific fields
                if "tipo de bien" in label:
                    property_data["property_type"] = value
                elif "provincia" in label:
                    # Add province to additional attributes
                    pass
                elif "superficie" in label:
                    # Parse area measurements (could be M², M2, or HAS + M²)
                    if "HAS" in value.upper():
                        # Extract hectares and M² separately
                        has_match = re.search(
                            r"([\d,]+(?:\.\d+)?)\s*HAS", value.upper()
                        )
                        m2_match = re.search(
                            r"([\d,]+(?:\.\d+)?)\s*M[²2]", value.upper()
                        )

                        if has_match:
                            try:
                                # Remove commas before converting to float
                                property_data["hectares"] = float(
                                    has_match.group(1).replace(",", "")
                                )
                            except ValueError:
                                pass

                        if m2_match:
                            try:
                                # Remove commas before converting to float
                                property_data["area_m2"] = float(
                                    m2_match.group(1).replace(",", "")
                                )
                            except ValueError:
                                pass
                    else:
                        # Just M²
                        area_match = re.search(
                            r"([\d,]+(?:\.\d+)?)\s*M[²2]", value.upper()
                        )
                        if area_match:
                            try:
                                # Remove commas before converting to float
                                property_data["area_m2"] = float(
                                    area_match.group(1).replace(",", "")
                                )
                            except ValueError:
                                pass

        # Extract images from product gallery using href attributes
        images = []
        link_elems = soup.select(".woocommerce-product-gallery__wrapper a")

        for i, link in enumerate(link_elems, 1):
            href = link.get("href")
            if href:
                # Get alt text from the img inside the link for title
                # img_elem = link.select_one("img")
                # alt_text = img_elem.get("alt", "") if img_elem else ""

                images.append(
                    {
                        "source_url": href,
                        "title": f"Imagen #{i} de bien en venta ubicado en {property_data.get('address', 'N/A')} con el precio {property_data.get('price', 'N/A')}"[
                            :255
                        ],
                    }
                )

        property_data["images"] = images

        # Extract product meta information
        product_meta = {}
        meta_elem = soup.select_one(".product_meta")
        if meta_elem:
            # Extract SKU
            sku_elem = meta_elem.select_one(".sku_wrapper .sku")
            if sku_elem:
                product_meta["sku"] = sku_elem.get_text(strip=True)

            # Extract category
            category_elem = meta_elem.select_one(".posted_in a")
            if category_elem:
                product_meta["category"] = category_elem.get_text(strip=True)

            # Extract all brands/marcas
            marca_links = meta_elem.select("span.posted_in a[href*='/marca/']")
            if marca_links:
                marcas = [link.get_text(strip=True) for link in marca_links]
                product_meta["marcas"] = marcas

        # Store additional raw attributes including title, address, table data, and product meta
        additional_attrs = {}

        # Add title to additional_attrs if available
        if title_text:
            additional_attrs["title"] = title_text

        # Add all table details to additional_attrs
        additional_attrs.update(details)

        # Add product meta information to additional_attrs
        additional_attrs.update(product_meta)

        # Add additional description to additional_attrs if exists
        if additional_description:
            additional_attrs["additional_description"] = additional_description
            # Also append to main description field
            if property_data.get("description"):
                property_data["description"] += f" {additional_description}"
            else:
                property_data["description"] = additional_description

        property_data["additional_attrs"] = additional_attrs

        if property_data.get("address"):
            property_data["address"] = property_data.get("address", "")[:255]

        logger.info(
            f"Successfully scraped property {property_data.get('property_id', 'unknown')}"
        )

        return property_data

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None


@app.cell
def _():
    scrape_property_page_banco_nacional(
        {
            "link": "https://www.banconal.com.pa/product/las-tablas/",
            "id": "7fc5a24f-896c-4830-8bc1-8349f270b0f5",
        }
    )
    return


@app.function
def generate_flow_run_name():
    date = datetime.datetime.now(datetime.timezone.utc)
    return f"banco_nacional_repossessed_assets_{date:%Y-%m-%d_%H-%M-%S}"


@app.function
@flow(
    description="Scrape Banco Nacional repossessed assets",
    flow_run_name=generate_flow_run_name,
)
async def banco_nacional_repossessed_assets():
    """Main flow to sync Banco Nacional repossessed assets links with Directus and scrape property data."""
    logger = get_run_logger()

    # Get all scraped links from Banco Nacional
    scraped_links = fetch_all_urls()
    scraped_links_set = set(scraped_links)

    # Get existing links from Directus
    existing_links = await get_existing_links_from_directus("banco-nacional")

    # Compare and find differences
    new_links = list(scraped_links_set - existing_links)

    logger.info(f"Found {len(new_links)} new links to add for Banco Nacional")

    # Sync with Directus
    await add_new_links_to_directus(new_links, "banco-nacional")

    logger.info("Link sync completed successfully")

    # Get unscraped links
    unscraped_links = await get_unscraped_links_from_directus("banco-nacional")

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

        scraping_futures = scrape_property_page_banco_nacional.map(batch)

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


@app.cell(disabled=True)
async def _():
    await banco_nacional_repossessed_assets()
    return


if __name__ == "__main__":
    app.run()
