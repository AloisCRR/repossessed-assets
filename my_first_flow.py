# my_first_flow.py
from prefect import flow, get_run_logger


@flow(log_prints=True)
def hello_world_flow(name: str = "World"):
    """
    A simple flow that prints a greeting.
    """
    logger = get_run_logger()
    logger.info(f"Hello, {name}!")
    print(f"Hello from Prefect, {name}! 🚀")


if __name__ == "__main__":
    # This part is useful for local testing but not used by the deployment.
    hello_world_flow("Local Test")
