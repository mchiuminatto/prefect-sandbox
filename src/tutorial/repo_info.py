import httpx
from prefect import flow, get_run_logger


@flow
def get_repo_info(repo_name: str = "PrefectHQ/prefect"):
    url = f"https://api.github.com/repos/{repo_name}"
    response = httpx.get(url)
    response.raise_for_status()  # raises a error for any status different from 2xx
    repo = response.json()
    logger = get_run_logger()
    logger.info("%s repository statistics 🤓:", repo_name)
    logger.info(f"Stars 🌠 : %d", repo["stargazers_count"])
    logger.info(f"Forks 🍴 : %d", repo["forks_count"])

if __name__ == "__main__":
    get_repo_info("mchiuminatto/triple_barrier")
    