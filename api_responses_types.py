from typing import List, TypedDict


class ScrapeDaum(TypedDict):
    id: str


class ScrapedImage(TypedDict):
    id: str
    source_url: str


class RepossessedAssetsLink(TypedDict):
    company: str
    id: str
    link: str
    scrape_data: List[ScrapeDaum]
    scraped_images: List[ScrapedImage]


class Data(TypedDict):
    repossessed_assets_links: List[RepossessedAssetsLink]


class RepossessedAssetStaleLinksGraphQL(TypedDict):
    data: Data
