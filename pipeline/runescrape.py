import requests
import backoff
import logging
import pandas as pd
from rich.progress import track
import click
import time

from clickhouse_driver import Client


def make_client() -> Client:
    return Client(host="localhost")


class MissingPlayer(Exception):
    ...


class ThrottleError(Exception):
    ...


API = "http://localhost:8080"


@backoff.on_exception(
    backoff.constant,
    requests.HTTPError,
    on_backoff=logging.debug,
    max_tries=2,
    jitter=None,
    interval=10,
)
def download_page(activity: str, page_number: int) -> requests.Response:
    response = requests.get(f"{API}/activity/{activity}?page={page_number}", timeout=10)
    response.raise_for_status()
    return response.json()


@backoff.on_exception(
    backoff.expo,
    (requests.HTTPError, ThrottleError),
    on_backoff=logging.debug,
    max_tries=5,
    jitter=None,
)
def download_player(player_name: str) -> dict:
    response = requests.get(f"{API}/player/{player_name}", timeout=10)
    if response.status_code == 500:
        raise MissingPlayer()
    elif response.status_code == 503:
        raise ThrottleError()
    response.raise_for_status()
    return response.json()


def scoreboard_json_name(activity: str) -> str:
    return f"data/hiscores/activity/{activity}.json"


def players_json_name(activity: str) -> str:
    return f"data/hiscores/player/{activity}-players.json"


def get_max_page(client: Client, activity: str) -> int:
    scoreboard_file = scoreboard_json_name(activity)
    query = f"select max(page) from file('{scoreboard_file}', JSONEachRow)"
    ((max_page,),) = client.execute(query)
    return max_page


@click.command()
@click.option("--activity", "-a", type=str)
@click.option("--page-goal", "-g", type=int, default=10_000)
def scrape(activity: str, page_goal: int):
    scoreboard_file_name = scoreboard_json_name(activity)
    player_file_name = players_json_name(activity)

    # so long as we've not met our page goal, keep going
    while (max_page := get_max_page(make_client(), activity)) < page_goal:
        start_from = max_page + 1
        print(f"Resuming scraping from page {start_from}")

        # pull more scoreboard results
        with open(scoreboard_file_name, "a") as fp:
            for page_number in track(
                range(start_from, page_goal + 1), description=f"Scraping {activity}..."
            ):
                try:
                    data = download_page(activity, page_number)
                    df = pd.DataFrame.from_dict(data)
                    df["page"] = page_number
                    df.to_json(fp, orient="records", lines=True, mode="a")
                except Exception:
                    break

        print("Taking a 60 second break...")
        time.sleep(60)

        # now try pulling player data
        query = f"""
            select name from file('{scoreboard_file_name}', JSONEachRow)
            where name not in (select name from file('{player_file_name}', JSONEachRow))
            order by name
            """

        players_to_do = [name for name, in make_client().execute(query)]

        with open(player_file_name, "a") as outfile:
            for player_name in track(
                players_to_do,
                description=f"Downloading {len(players_to_do)} players...",
            ):
                try:
                    player_stats = download_player(player_name)
                    row = {"name": player_name, **player_stats}
                    player_df = pd.DataFrame.from_dict([row])
                    player_df.to_json(
                        outfile,
                        orient="records",
                        lines=True,
                        mode="a",
                    )
                except Exception:
                    pass


if __name__ == "__main__":
    scrape()
