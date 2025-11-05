# deploy.py
from my_first_flow import hello_world_flow  # Import the flow to get its metadata

# The .deploy() method is a convenient way to create a deployment object
# It combines the flow's definition with runtime configuration.
if __name__ == "__main__":
    # `from_source` tells the worker where to get the code from.
    # `entrypoint` specifies the file and function to run.
    hello_world_flow.from_source(
        source="https://github.com/AloisCRR/repossessed-assets",
        entrypoint="my_first_flow.py:hello_world_flow",
    ).deploy(
        name="hello-world-github-deployment",
        work_pool_name="local-pool",  # This MUST match your worker's pool
        tags=["tutorial", "github"],
    )
