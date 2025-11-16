import marimo

__generated_with = "0.17.7"
app = marimo.App(width="columns", app_title="Banesco Repossessed Assets")

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

    from directus_tasks import (
        add_new_links_to_directus,
        get_existing_links_from_directus,
        get_unscraped_links_from_directus,
        mark_link_as_scraped,
        save_property_data,
    )


@app.function
@task(
    name="Fetch All Banesco URLs",
    description="Fetch all property URLs from Banesco repossessed assets catalog.",
    task_run_name="banesco-fetch-urls-from-catalog",
)
def fetch_all_banesco_urls():
    """Fetch all property URLs from Banesco repossessed assets catalog."""
    logger = get_run_logger()
    base_url = "https://www.banesco.com.pa/banesco-bienes/"
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
    page_num = 1

    while True:
        if page_num == 1:
            page_url = base_url
        else:
            page_url = f"{base_url}page/{page_num}/"

        try:
            logger.info(f"Fetching page {page_num}: {page_url}")
            response = requests.get(page_url, headers=headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            if soup.select_one(".error-404"):
                logger.info(f"No more pages found. Reached page {page_num}.")
                break

            property_links = soup.select(
                "li.product a.woocommerce-LoopProduct-link"
            )
            if not property_links:
                logger.info(f"No property links found on page {page_num}.")
                break

            for link in property_links:
                href = link.get("href")
                if href:
                    all_links.append(href)

            logger.info(
                f"Page {page_num}: Found {len(property_links)} property links"
            )
            page_num += 1
            time.sleep(1)

        except requests.RequestException as e:
            logger.error(f"Error fetching page {page_url}: {e}")
            break
        except Exception as e:
            logger.error(f"Unexpected error processing page {page_url}: {e}")
            break

    unique_links = list(dict.fromkeys(all_links))
    logger.info(f"Total: Found {len(unique_links)} unique property links")
    return unique_links


@app.function
@task(
    name="Scrape Property Page - Banesco",
    description="Scrape individual property page and extract all data.",
    task_run_name="banesco-scrape-property-{link_data[id]}",
)
def scrape_property_page_banesco(link_data: Dict[str, str]) -> Optional[Dict]:
    """Scrape individual property page and extract all data."""
    logger = get_run_logger()
    link_id = link_data["id"]
    url = link_data["link"]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0",
    }

    try:
        logger.info(f"Scraping property page: {url}")
        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        property_data = {"link_id": link_id, "status": "active", "price": 0}

        # Extract Title
        title_elem = soup.select_one("h1.product_title.entry-title")
        if title_elem:
            property_data["title"] = title_elem.get_text(strip=True)

        # Extract Price
        price_elem = soup.select_one(
            "p.price .woocommerce-Price-amount.amount"
        )
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            price_clean = re.sub(r"[^\d.]", "", price_text)
            try:
                property_data["price"] = float(price_clean)
                property_data["currency"] = "PAB"
            except ValueError:
                pass

        # Extract Property ID
        sku_elem = soup.select_one(".sku_wrapper .sku")
        if sku_elem:
            property_data["property_id"] = sku_elem.get_text(strip=True)

        # Extract Property Type
        category_elem = soup.select_one(".posted_in a")
        if category_elem:
            property_data["property_type"] = category_elem.get_text(strip=True)

        # Extract Address
        address_elem = soup.select_one(".item-location-oneline")
        if address_elem:
            property_data["address"] = address_elem.get_text(strip=True)

        # Extract Description
        benefits_elem = soup.select_one("#tab-custom_tab_item_benefits")
        location_elem = soup.select_one("#tab-custom_tab_property_location")
        description = []
        if benefits_elem:
            description.append(benefits_elem.get_text(strip=True))
        if location_elem:
            description.append(location_elem.get_text(strip=True))
        property_data["description"] = ", ".join(description)

        # Extract Images
        images = []
        image_elems = soup.select(
            ".wpgs-for .woocommerce-product-gallery__image a"
        )
        for i, img in enumerate(image_elems, 1):
            src = img.get("href")
            if src:
                images.append(
                    {
                        "source_url": src,
                        "title": f"Imagen #{i} de bien en venta ubicado en {property_data.get('address', 'N/A')} con el precio {property_data.get('price', 'N/A')}",
                    }
                )
        property_data["images"] = images

        # Extract Coordinates
        script_tag = soup.find(
            "script", string=re.compile("function initMap()")
        )
        if script_tag:
            script_content = script_tag.string
            match = re.search(
                r"new google\.maps\.LatLng\(([^)]+)\)", script_content
            )
            if match:
                coords_str = match.group(1)
                lat_str, lon_str = None, None

                # Most reliable seems to be splitting by a comma followed by a dash
                parts = re.split(r",\s*(?=-)", coords_str)
                if len(parts) == 2:
                    lat_str = parts[0].replace(",", ".").strip()
                    lon_str = parts[1].replace(",", ".").strip()
                else:
                    # Fallback for standard comma separation
                    parts = coords_str.split(",")
                    if len(parts) == 2:
                        lat_str = parts[0].strip()
                        lon_str = parts[1].strip()

                if lat_str and lon_str:
                    try:
                        lat = float(lat_str)
                        lon = float(lon_str)

                        property_data["latitude"] = str(lat)
                        property_data["longitude"] = str(lon)

                        # Safety measure: check if coordinates are within Panama's bounds
                        if 7.0 < lat < 10.0 and -83.0 < lon < -77.0:
                            property_data["geog"] = {
                                "type": "Point",
                                "coordinates": [lon, lat],
                            }
                        else:
                            logger.warning(
                                f"Coordinates ({lat}, {lon}) for property at {url} are outside the expected range for Panama."
                            )
                    except ValueError:
                        logger.warning(
                            f"Could not convert coordinates '{lat_str}', '{lon_str}' to float for {url}"
                        )
                        pass

        # Extract Attributes
        metadata_items = soup.select(".item-metadata span")
        for item in metadata_items:
            icon = item.select_one("i")
            if not icon:
                continue

            text_content = item.get_text(strip=True)

            if "fa-ruler-combined" in icon.get(
                "class", []
            ):  # built area or land area for terrenos
                if "has" in text_content.lower():
                    # Format: 3has + 5,178.15 m2
                    hectares_match = re.search(
                        r"(\d+[\d,.]*)\s*has", text_content, re.IGNORECASE
                    )
                    m2_match = re.search(
                        r"(\d+[\d,.]*)\s*m2", text_content, re.IGNORECASE
                    )
                    total_m2 = 0
                    if hectares_match:
                        try:
                            hectares = float(
                                hectares_match.group(1).replace(",", "")
                            )
                            total_m2 += hectares * 10000
                            property_data["hectares"] = hectares
                        except (ValueError, IndexError):
                            pass
                    if m2_match:
                        try:
                            m2 = float(m2_match.group(1).replace(",", ""))
                            total_m2 += m2
                        except (ValueError, IndexError):
                            pass
                    if total_m2 > 0:
                        property_data["area_m2"] = total_m2
                else:
                    match = re.search(r"([\d,.]+)", text_content)
                    if match:
                        try:
                            value_str = match.group(1).replace(",", "")
                            if value_str:
                                property_data["built_area"] = float(value_str)
                        except ValueError:
                            pass
            elif "fa-arrows-up-down-left-right" in icon.get(
                "class", []
            ):  # land area
                match = re.search(r"([\d,.]+)", text_content)
                if match:
                    try:
                        value_str = match.group(1).replace(",", "")
                        if value_str:
                            property_data["area_m2"] = float(value_str)
                    except ValueError:
                        pass
            elif "fa-bed" in icon.get("class", []):
                try:
                    value_str = re.sub(r"\D", "", text_content)
                    if value_str:
                        property_data["bedrooms"] = int(value_str)
                except ValueError:
                    pass
            elif "fa-bath" in icon.get("class", []):
                try:
                    value_str = re.sub(r"\D", "", text_content)
                    if value_str:
                        property_data["bathrooms"] = int(value_str)
                except ValueError:
                    pass
            elif "fa-square-parking" in icon.get("class", []):
                try:
                    value_str = re.sub(r"\D", "", text_content)
                    if value_str:
                        property_data["parking"] = int(value_str)
                except ValueError:
                    pass

        logger.info(
            f"Successfully scraped property {property_data.get('property_id', 'unknown')}"
        )
        return property_data

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None


@app.cell(disabled=True)
def _():
    scrape_property_page_banesco(
        {
            "link": "https://www.banesco.com.pa/banesco-bienes/oficinap-h-torre-de-las-americas-torre-aunidad-lote-1602-a/",
            "id": "e7b1d299-3368-46d6-bb49-0b542e609136",
        }
    )
    return


@app.function
def generate_flow_run_name_banesco():
    date = datetime.datetime.now(datetime.timezone.utc)
    return f"banesco_repossessed_assets_{date:%Y-%m-%d_%H-%M-%S}"


@app.function
@flow(
    description="Scrape Banesco repossessed assets",
    flow_run_name=generate_flow_run_name_banesco,
)
async def banesco_repossessed_assets():
    """Main flow to sync Banesco repossessed assets links with Directus and scrape property data."""
    logger = get_run_logger()

    scraped_links = fetch_all_banesco_urls()

    existing_links = await get_existing_links_from_directus("banesco")

    scraped_links_set = set(scraped_links)

    new_links = list(scraped_links_set - existing_links)

    logger.info(f"Found {len(new_links)} new links to add for Banesco")

    if new_links:
        await add_new_links_to_directus(new_links, "banesco")

    logger.info("Link sync completed successfully")

    # Get unscraped links
    unscraped_links = await get_unscraped_links_from_directus("banesco")

    if not unscraped_links:
        logger.info("No unscraped links found")
        return

    logger.info(f"Found {len(unscraped_links)} unscraped links to process")

    # Process links in batches of 5
    batch_size = 5
    total_processed = 0

    for i in range(0, len(unscraped_links), batch_size):
        batch = unscraped_links[i : i + batch_size]

        logger.info(
            f"Processing batch {i // batch_size + 1}: {len(batch)} links"
        )

        scraping_futures = scrape_property_page_banesco.map(batch)

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
                            logger.warning(
                                f"Failed to save data for link {link_id}"
                            )
                    else:
                        logger.warning("No data scraped from successful task")
                except Exception as e:
                    logger.error(
                        f"Error processing successful task result: {e}"
                    )
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
    await banesco_repossessed_assets()
    return


if __name__ == "__main__":
    app.run()
