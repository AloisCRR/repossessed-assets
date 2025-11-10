import marimo

__generated_with = "0.17.7"
app = marimo.App(width="columns", app_title="Re-Scrape Stale Links")

with app.setup:
    import marimo as mo
    import datetime

    from prefect import flow, get_run_logger

    from directus_tasks import (
        get_all_stale_links_from_directus,
        mark_link_as_fresh,
        update_property_data,
    )

    # Import scraper functions from existing files
    from caja_de_ahorros import scrape_property_page_caja_de_ahorros
    from banco_general import scrape_property_page_banco_general
    from global_bank import scrape_property_page_global_bank


@app.function
def generate_flow_run_name():
    date = datetime.datetime.now(datetime.timezone.utc)
    return f"reprocess_stale_links_{date:%Y-%m-%d_%H-%M-%S}"


@app.function
@flow(
    description="Reprocess stale repossessed assets links",
    flow_run_name=generate_flow_run_name,
)
async def reprocess_stale_links():
    """Main flow to reprocess stale links and update property data."""
    logger = get_run_logger()

    try:
        # Get all stale links
        stale_links = await get_all_stale_links_from_directus()

        if not stale_links or not stale_links.get("data", {}).get(
            "repossessed_assets_links", []
        ):
            logger.info("No stale links found")
            return

        links_to_process = stale_links["data"]["repossessed_assets_links"]
        logger.info(f"Found {len(links_to_process)} stale links to process")

        total_processed = 0
        total_failed = 0

        for link_data in links_to_process:
            company = link_data.get("company", "")
            link = link_data.get("link", "")
            link_id = link_data.get("id", "")

            try:
                if not link or not link_id:
                    logger.warning(
                        f"Missing link or link_id for company {company}"
                    )
                    total_failed += 1
                    continue

                logger.info(f"Processing link {link_id} for company {company}")
                link_to_scrape = {"link": link, "id": link_id}

                # Route to appropriate scraper based on company
                scraped_data = None

                if company == "caja-de-ahorros":
                    scraped_data = scrape_property_page_caja_de_ahorros(
                        link_to_scrape
                    )
                elif company == "banco-general":
                    scraped_data = scrape_property_page_banco_general(
                        link_to_scrape
                    )
                elif company == "global-bank":
                    scraped_data = scrape_property_page_global_bank(
                        link_to_scrape
                    )
                else:
                    logger.warning(
                        f"No scraper available for company: {company}"
                    )
                    total_failed += 1
                    continue

                if not scraped_data:
                    logger.error(f"No data scraped for link {link_id}")
                    total_failed += 1
                    continue

                # Get the property data ID
                scrape_data_list = link_data.get("scrape_data", [])
                if not scrape_data_list:
                    logger.error(f"No property data found for link {link_id}")
                    total_failed += 1
                    continue

                scraped_data_id = scrape_data_list[0].get("id", "")
                if not scraped_data_id:
                    logger.error(
                        f"No property data ID found for link {link_id}"
                    )
                    total_failed += 1
                    continue

                # Extract existing image URLs
                existing_image_urls = [
                    item["source_url"]
                    for item in link_data.get("scraped_images", [])
                    if item.get("source_url")
                ]

                # Update property data
                try:
                    update_success = await update_property_data(
                        scraped_data_id=scraped_data_id,
                        property_data=scraped_data,
                        existing_image_urls=existing_image_urls,
                    )

                    if update_success:
                        # Mark link as fresh
                        mark_success = await mark_link_as_fresh(link_id)
                        if mark_success:
                            logger.info(
                                f"Successfully processed link {link_id}"
                            )
                            total_processed += 1
                        else:
                            logger.warning(
                                f"Failed to mark link {link_id} as fresh"
                            )
                            total_failed += 1
                    else:
                        logger.warning(
                            f"Failed to update data for link {link_id}"
                        )
                        total_failed += 1
                except Exception as update_error:
                    logger.error(
                        f"Update failed for link {link_id}: {update_error}"
                    )
                    total_failed += 1

            except Exception as link_error:
                logger.error(f"Error processing link {link_id}: {link_error}")
                total_failed += 1

        logger.info(
            f"Reprocessing completed. Processed: {total_processed}, Failed: {total_failed}, Total: {len(links_to_process)} or {round(total_processed / len(links_to_process) * 100, 2)}%"
        )

    except Exception as flow_error:
        logger.error(f"Flow failed: {flow_error}")
        raise


@app.cell
async def _():
    await reprocess_stale_links()
    return


if __name__ == "__main__":
    app.run()
