# my_complex_flow.py
import httpx
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta


# Define a task for the EXTRACT step
@task(retries=3, retry_delay_seconds=5)
def fetch_repos(username: str):
    """
    Fetches all public repositories for a given GitHub username.
    Retries up to 3 times if the API call fails.
    """
    logger = get_run_logger()
    url = f"https://api.github.com/users/{username}/repos"
    logger.info(f"Fetching repositories from {url}...")

    response = httpx.get(url)
    response.raise_for_status()  # Raises an exception for 4xx or 5xx status codes
    repos = response.json()
    logger.info(f"Found {len(repos)} public repositories.")
    return repos


# Define a task for the TRANSFORM step
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def find_most_popular_repo(repos: list):
    """
    Finds the repository with the most stars.
    This task is cached for 1 hour; if the list of repos hasn't changed,
    this task will not re-run and the cached result will be returned.
    """
    logger = get_run_logger()
    if not repos:
        logger.warning("No repositories found.")
        return None

    logger.info("Finding the most popular repository...")
    most_popular = max(repos, key=lambda repo: repo["stargazers_count"])

    result = {
        "name": most_popular["name"],
        "stars": most_popular["stargazers_count"],
        "url": most_popular["html_url"],
    }
    return result


# Define a task for the LOAD (report) step
@task
def report_stats(popular_repo: dict):
    """
    Logs a summary of the most popular repository.
    """
    logger = get_run_logger()
    if popular_repo:
        logger.info("🏆 Most Popular Repository Stats 🏆")
        logger.info(f"Name: {popular_repo['name']}")
        logger.info(f"Stars: {popular_repo['stars']} ⭐")
        logger.info(f"URL: {popular_repo['url']}")
    else:
        logger.info("No repository stats to report.")


# Define the main flow that orchestrates the tasks
@flow(log_prints=True)
def github_stats_flow(username: str = "PrefectHQ"):
    """
    A flow that fetches GitHub stats, finds the most popular
    repository, and reports the result.
    """
    print(f"--- Starting GitHub stats flow for user: {username} ---")
    repos = fetch_repos(username=username)
    popular_repo = find_most_popular_repo(repos=repos)
    report_stats(popular_repo=popular_repo)
    print("--- Flow finished successfully! ---")


# This allows you to run the flow locally for testing
if __name__ == "__main__":
    github_stats_flow(username="AloisCRR")
