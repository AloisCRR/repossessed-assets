import marimo

__generated_with = "0.17.7"
app = marimo.App(
    width="columns",
    app_title="Caja de Ahorros Repossessed Assets",
)

with app.setup:
    import datetime
    import json
    import re
    from typing import Dict, Optional

    import requests
    from bs4 import BeautifulSoup
    from prefect import flow, get_run_logger, task
    from prefect.futures import wait
    from unidecode import unidecode

    from directus_tasks import (
        add_new_links_to_directus,
        get_existing_links_from_directus,
        get_unscraped_links_from_directus,
        save_property_data,
        mark_link_as_scraped,
    )


@app.function
@task(
    name="Fetch All URLs",
    description="Fetch all property URLs from Caja de Ahorros repossessed assets catalog.",
    task_run_name="caja-de-ahorros-fetch-urls-from-catalog",
)
def fetch_all_urls():
    logger = get_run_logger()
    catalog_url = "https://www.cajadeahorros.com.pa/propiedades/bienes-reposeidos/"

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
                        logger.error(f"Failed to parse allProperties JSON: {e}")
                        continue

        if not all_properties_data:
            logger.error("Could not find allProperties variable in any script tag")
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
    name="Scrape Property Page - Caja de Ahorros",
    description="Scrape individual property page and extract all data.",
    task_run_name="caja-de-ahorros-scrape-property-{link_data[id]}",
)
def scrape_property_page_caja_de_ahorros(
    link_data: Dict[str, str],
) -> Optional[Dict]:
    """Scrape individual property page and extract all data."""
    logger = get_run_logger()
    link_id = link_data["id"]
    url = link_data["link"]

    cookies = {
        "PORTAL-XSESSIONID": "1762648165.101.2206.779811|29e68a15732949f1942f74c137980c8c",
        "pum-50286": "true",
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
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
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

        # Extract property title and ID
        title_elem = soup.select_one(".property-title h1.semibold")

        if title_elem:
            title_text = title_elem.get_text(strip=True)
            # property_data["title"] = title_text

            id_match = re.match(r"F\.\s*(\d+)", title_text)

            if id_match:
                property_data["property_id"] = id_match.group(1)

            # Split the title by comma to separate property ID from address
            # title_parts = title_text.split(",", 1)

            # if title_parts:
            #     # Extract property ID from the first part and remove "F. " prefix
            #     # property_id = title_parts[0].replace("F. ", "").strip()
            #     # property_data["property_id"] = property_id

            #     # print(title_parts)

            #     # Use the remaining part as address if available
            #     if len(title_parts) > 1:
            #         property_data["address"] = title_parts[1].strip()

            #     else:
            #         # If no comma was found, use the full title as address
            #         property_data["address"] = title_text

        address_elem = soup.select_one(".entry-content p")

        if address_elem:
            property_data["address"] = address_elem.get_text(strip=True)

        # Extract price
        price_elem = soup.select_one(".property-price")
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            # Remove all characters except digits and dots
            price_clean = re.sub(r"[^\d.]", "", price_text)
            try:
                property_data["price"] = float(price_clean) if price_clean else None
                property_data["currency"] = "PAB"  # Panamanian Balboa
            except ValueError:
                property_data["price"] = None

        # Extract property information from details section
        details = {}
        detail_items = soup.select(".property-info ul li")
        for item in detail_items:
            text = item.get_text(strip=True)
            # Split on the first colon to get label and value
            if ":" in text:
                parts = text.split(":", 1)
                if len(parts) == 2:
                    label = unidecode(parts[0].strip().lower())
                    value = parts[1].strip()

                    if value:
                        details[label] = value
            else:
                details[text] = "true"

        # Map specific fields
        if "tipo de propiedad" in details:
            property_data["property_type"] = details["tipo de propiedad"]

        # if "Provincia" in details:
        #     property_data["address"] = details["Provincia"]

        # if "Sector" in details:
        #     if property_data.get("address"):
        #         property_data["address"] += f", {details['Sector']}"
        #     else:
        #         property_data["address"] = details["Sector"]

        # Extract area measurements
        if "area de construccion" in details:
            area_text = details["area de construccion"]
            area_clean = re.sub(r"[^\d.]", "", area_text)
            try:
                property_data["built_area"] = float(area_clean) if area_clean else None
            except ValueError:
                pass

        if "metros del terreno" in details:
            area_text = details["metros del terreno"]
            area_clean = re.sub(r"[^\d.]", "", area_text)
            try:
                property_data["area_m2"] = float(area_clean) if area_clean else None
            except ValueError:
                pass

        if "hectareas" in details:
            hectares_text = details["hectareas"]
            hectares_clean = re.sub(r"[^\d.]", "", hectares_text)
            try:
                property_data["hectares"] = (
                    float(hectares_clean) if hectares_clean else None
                )
            except ValueError:
                pass

        # Extract room counts
        if "habitaciones" in details:
            try:
                property_data["bedrooms"] = int(details["habitaciones"])
            except ValueError:
                pass

        if "banos" in details:
            try:
                property_data["bathrooms"] = int(round(float(details["banos"])))
            except ValueError:
                pass

        # Extract coordinates from Google Maps iframe
        iframe_elem = soup.select_one(".property-map iframe")
        if iframe_elem:
            src = iframe_elem.get("src", "")
            # Extract coordinates from URL like: https://maps.google.com/maps?q=8.373917,-80.1355&hl=es&z=14&output=embed
            coord_match = re.search(pattern=r"q=([-\d.]+),([-\d.]+)", string=src)

            if coord_match:
                try:
                    lat = float(coord_match.group(1))
                    lon = float(coord_match.group(2))
                    property_data["latitude"] = str(lat)
                    property_data["longitude"] = str(lon)
                    property_data["geog"] = {
                        "type": "Point",
                        "coordinates": [lon, lat],
                    }
                except ValueError:
                    pass

        # Extract amenities
        amenities = soup.select("h3:-soup-contains('Amenidades') + ul li")

        amenities_dict = {}

        feature_mapping = {
            "terrace": "terraza",
            "laundry": "lavanderia",
            "social_area": "area social",
            "security": "seguridad",
            "balcony": "balcon",
            "swimming_pool": "piscina",
            "parking": "estacionamiento",
        }

        for amenity in amenities:
            amenity_text = amenity.get_text(strip=True)

            for field_name, keyword in feature_mapping.items():
                if keyword in unidecode(amenity_text.lower()):
                    if keyword == "estacionamiento":
                        property_data[field_name] = 1
                    else:
                        property_data[field_name] = True

            if amenity_text:
                if ":" in amenity_text:
                    parts = amenity_text.split(":", 1)

                    if len(parts) == 2:
                        label = parts[0].strip()
                        value = parts[1].strip()

                        if value:
                            amenities_dict[label] = value

        # Extract images from mobile gallery
        images = []
        img_elems = soup.select(".image-gallery img")

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
        additional_attrs = {**details, **amenities_dict}

        # for label, value in details.items():
        #     additional_attrs[label] = value

        # for amenity in amenities:
        #     amenity_text = amenity.get_text(strip=True)
        #     if amenity_text:
        #         additional_attrs[amenity_text] = "true"

        amenities_text = [amenity.get_text(strip=True) for amenity in amenities]

        additional_attrs["Amenidades"] = ", ".join(filter(None, amenities_text))

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
    scrape_property_page_caja_de_ahorros(
        {
            "link": "https://www.cajadeahorros.com.pa/propiedad/f-328236-juan-diaz-ph-sunset-coast-casa-n35/",
            "id": "ab3ce2be-ebb9-4069-acd2-592887b7d2ff",
        }
    )
    return


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
    """Main flow to sync Caja de Ahorros repossessed assets links with Directus and scrape property data."""
    logger = get_run_logger()

    # Get all scraped links from Caja de Ahorros
    scraped_links = fetch_all_urls()
    scraped_links_set = set(scraped_links)

    # Get existing links from Directus
    existing_links = await get_existing_links_from_directus("caja-de-ahorros")

    # Compare and find differences
    new_links = list(scraped_links_set - existing_links)

    logger.info(f"Found {len(new_links)} new links to add for Caja de Ahorros")

    # Sync with Directus
    await add_new_links_to_directus(new_links, "caja-de-ahorros")

    logger.info("Link sync completed successfully")

    # Get unscraped links
    unscraped_links = await get_unscraped_links_from_directus("caja-de-ahorros")

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

        scraping_futures = scrape_property_page_caja_de_ahorros.map(batch)

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
    await caja_de_ahorros_repossessed_assets()
    return


if __name__ == "__main__":
    app.run()
