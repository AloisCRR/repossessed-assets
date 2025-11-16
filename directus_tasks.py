import os
from typing import Dict, List, Set, TypedDict

import aiohttp
from prefect import get_run_logger, task
from api_responses_types import RepossessedAssetStaleLinksGraphQL


@task(
    name="Get Existing Links from Directus",
    description="Get all existing links from Directus repossessed_assets_links collection.",
    task_run_name="{company}-get-existing-links-directus",
)
async def get_existing_links_from_directus(company: str) -> Set[str]:
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
            f"{directus_url}/items/repossessed_assets_links?filter[company][_eq]={company}&limit=-1&fields=link",
            headers=headers,
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(
                    f"Failed to fetch existing links: {response.status} - {error_text}"
                )
                raise Exception(f"Directus API error: {response.status} - {error_text}")

            data = await response.json()
            existing_links = {item["link"] for item in data["data"]}
            logger.info(
                f"Found {len(existing_links)} existing links in Directus for {company}"
            )
            return existing_links


@task(
    name="Add New Links to Directus",
    description="Add new links to Directus repossessed_assets_links collection.",
    task_run_name="{company}-add-new-links-to-directus",
)
async def add_new_links_to_directus(new_links: List[str], company: str) -> None:
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
        {"link": link, "is_scraped": False, "company": company} for link in new_links
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
                raise Exception(f"Directus API error: {response.status} - {error_text}")

            result = await response.json()
            logger.info(
                f"Added {len(result['data'])} new links to Directus for {company}"
            )


@task(
    name="Get Unscraped Links from Directus",
    description="Get unscraped links from Directus repossessed_assets_links collection.",
    task_run_name="{company}-get-unscraped-links-directus",
)
async def get_unscraped_links_from_directus(company: str) -> List[Dict[str, str]]:
    """Get unscraped links from Directus repossessed_assets_links collection."""
    logger = get_run_logger()
    directus_url = os.environ["DIRECTUS_URL"]
    directus_token = os.environ["DIRECTUS_TOKEN"]

    headers = {
        "Authorization": f"Bearer {directus_token}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        # Get unscraped items with id and link
        async with session.get(
            f"{directus_url}/items/repossessed_assets_links?filter[company][_eq]={company}&filter[is_scraped][_eq]=false&limit=-1&fields=id,link",
            headers=headers,
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(
                    f"Failed to fetch unscraped links: {response.status} - {error_text}"
                )
                raise Exception(f"Directus API error: {response.status} - {error_text}")

            data = await response.json()
            unscraped_links = data["data"]
            logger.info(
                f"Found {len(unscraped_links)} unscraped links in Directus for {company}"
            )
            return unscraped_links


@task(
    name="Save Property Data",
    description="Save property data to Directus repossessed_assets_data collection.",
    task_run_name="save-property-{property_data[link_id]}",
    retries=3,
)
async def save_property_data(property_data: Dict) -> bool:
    """Save property data to Directus repossessed_assets_data collection."""
    logger = get_run_logger()
    directus_url = os.environ["DIRECTUS_URL"]
    directus_token = os.environ["DIRECTUS_TOKEN"]

    headers = {
        "Authorization": f"Bearer {directus_token}",
        "Content-Type": "application/json",
    }

    # Extract images for separate storage
    images = property_data.pop("images", [])
    link_id = property_data["link_id"]

    async with aiohttp.ClientSession() as session:
        # Save main property data
        async with session.post(
            f"{directus_url}/items/repossessed_assets_data",
            headers=headers,
            json=property_data,
        ) as response:
            if response.status not in [200, 201]:
                error_text = await response.text()
                logger.error(
                    f"Failed to save property data: {response.status} - {error_text}"
                )
                return False

            # Save images if any
            if images:
                # Remove duplicate images based on source_url
                seen_urls = set()

                unique_images = []

                for img in images:
                    if img["source_url"] not in seen_urls:
                        seen_urls.add(img["source_url"])
                        unique_images.append(img)

                image_items = [
                    {
                        "link_id": link_id,
                        "source_url": img["source_url"],
                        "title": img["title"],
                    }
                    for img in unique_images
                ]

                async with session.post(
                    f"{directus_url}/items/repossessed_assets_images",
                    headers=headers,
                    json=image_items,
                ) as img_response:
                    if img_response.status not in [200, 201]:
                        error_text = await img_response.text()
                        logger.warning(
                            f"Failed to save images: {img_response.status} - {error_text}"
                        )

            logger.info(f"Successfully saved property data for link {link_id}")
            return True


@task(
    name="Mark Link as Scraped",
    description="Mark link as scraped in Directus.",
    task_run_name="mark-link-{link_id}-scraped",
)
async def mark_link_as_scraped(link_id: str) -> bool:
    """Mark link as scraped in Directus."""
    logger = get_run_logger()
    directus_url = os.environ["DIRECTUS_URL"]
    directus_token = os.environ["DIRECTUS_TOKEN"]

    headers = {
        "Authorization": f"Bearer {directus_token}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.patch(
            f"{directus_url}/items/repossessed_assets_links/{link_id}",
            headers=headers,
            json={"is_scraped": True},
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(
                    f"Failed to mark link as scraped: {response.status} - {error_text}"
                )
                return False

            logger.info(f"Successfully marked link {link_id} as scraped")
            return True


@task(
    name="Get All Stale Links from Directus",
    description="Get all stale links from Directus using GraphQL with relational data.",
    task_run_name="get-all-stale-links-directus",
)
async def get_all_stale_links_from_directus() -> RepossessedAssetStaleLinksGraphQL:
    """Get all stale links from Directus using GraphQL with relational data."""
    logger = get_run_logger()
    directus_url = os.environ["DIRECTUS_URL"]
    directus_token = os.environ["DIRECTUS_TOKEN"]

    headers = {
        "Authorization": f"Bearer {directus_token}",
        "Content-Type": "application/json",
    }

    graphql_query = """
    query GetStaleRepossessedAssetsLinks {
      repossessed_assets_links(filter: { is_stale: { _eq: true } }) {
        company
        id
        link
        scrape_data {
          id
        }
        scraped_images {
          id
          source_url
        }
      }
    }
    """

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{directus_url}/graphql",
            headers=headers,
            json={"query": graphql_query},
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(
                    f"Failed to fetch stale links via GraphQL: {response.status} - {error_text}"
                )
                raise Exception(
                    f"Directus GraphQL error: {response.status} - {error_text}"
                )

            data = await response.json()

            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                raise Exception(f"GraphQL errors: {data['errors']}")

            stale_links = data["data"]["repossessed_assets_links"]
            logger.info(f"Found {len(stale_links)} stale links in Directus")
            return data  # Return full GraphQL response


@task(
    name="Mark Link as Fresh",
    description="Mark link as not stale in Directus after successful re-scraping.",
    task_run_name="mark-link-{link_id}-fresh",
)
async def mark_link_as_fresh(link_id: str) -> bool:
    """Mark link as not stale in Directus after successful re-scraping."""
    logger = get_run_logger()
    directus_url = os.environ["DIRECTUS_URL"]
    directus_token = os.environ["DIRECTUS_TOKEN"]

    headers = {
        "Authorization": f"Bearer {directus_token}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.patch(
            f"{directus_url}/items/repossessed_assets_links/{link_id}",
            headers=headers,
            json={"is_stale": False},
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(
                    f"Failed to mark link as fresh: {response.status} - {error_text}"
                )
                return False

            logger.info(f"Successfully marked link {link_id} as fresh")
            return True


class ExistingImages(TypedDict):
    id: str
    source_url: str


@task(
    name="Update Property Data",
    description="Update existing property data in Directus repossessed_assets_data collection.",
    task_run_name="update-property-{scraped_data_id}",
    retries=3,
)
async def update_property_data(
    scraped_data_id: str,
    property_data: Dict,
    existing_image_urls: List[str] = [],
) -> bool:
    """Update existing property data in Directus repossessed_assets_data collection."""
    logger = get_run_logger()
    directus_url = os.environ["DIRECTUS_URL"]
    directus_token = os.environ["DIRECTUS_TOKEN"]

    headers = {
        "Authorization": f"Bearer {directus_token}",
        "Content-Type": "application/json",
    }

    # Extract images for separate storage
    images = property_data.pop("images", [])
    link_id = property_data["link_id"]

    async with aiohttp.ClientSession() as session:
        # Update the existing property data
        async with session.patch(
            f"{directus_url}/items/repossessed_assets_data/{scraped_data_id}",
            headers=headers,
            json=property_data,
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(
                    f"Failed to update property data: {response.status} - {error_text}"
                )
                return False

            # Add new images if any
            if images:
                # Filter out images that already exist
                unique_new_images = []

                existing_urls_set = set(existing_image_urls)

                for img in images:
                    if img["source_url"] not in existing_urls_set:
                        unique_new_images.append(img)

                if unique_new_images:
                    # Prepare image items for insertion
                    image_items = [
                        {
                            "link_id": link_id,
                            "source_url": img["source_url"],
                            "title": img["title"],
                        }
                        for img in unique_new_images
                    ]

                    async with session.post(
                        f"{directus_url}/items/repossessed_assets_images",
                        headers=headers,
                        json=image_items,
                    ) as img_response:
                        if img_response.status not in [200, 201]:
                            error_text = await img_response.text()
                            logger.warning(
                                f"Failed to add new images: {img_response.status} - {error_text}"
                            )
                        else:
                            logger.info(
                                f"Successfully added {len(image_items)} new images for link {link_id}"
                            )

            logger.info(f"Successfully updated property data for link {link_id}")
            return True
