import httpx
from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from typing import Optional



@task(
        cache_key_fn=task_input_hash,  # function to calculate hash on inputs for cacheing 
        cache_expiration=timedelta(hours=1)  # cache expiration
        )
def get_url(url: str, params: Optional[dict[str, any]]=None):
    response = httpx.get(url, params=params)
    response.raise_for_status()
    return response.json()



@flow(retries=3, retry_delay_seconds=5, log_prints=True)
def get_repo_info(repo_name: str = "PrefectHQ/prefect"):
    url = f"https://api.github.com/repos/{repo_name}"
    # task
    repo_stats = get_url(url)

    print(f"{repo_name} repository statistics 🤓:")
    print(f"Stars 🌠 : {repo_stats['stargazers_count']}")
    print(f"Forks 🍴 : {repo_stats['forks_count']}")

if __name__ == "__main__":
    get_repo_info("mchiuminatto/triple_barrier")

