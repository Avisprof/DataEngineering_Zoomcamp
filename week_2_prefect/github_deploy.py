from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from web_to_gcs import etl_parent_flow

github_block = GitHub.load("zoom")

github_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="Github HW4",
    parameters={"year": 2020, "color":"green", "months":[11]},
    storage=github_block,
    entrypoint="./tree/main/web_to_gcs.py:etl_parent_flow"
)

if __name__ == '__main__':
    github_dep.apply()