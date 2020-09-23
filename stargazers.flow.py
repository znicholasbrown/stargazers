import prefect
from prefect import Flow, Task, Parameter, case
from prefect.client import Secret
from prefect.tasks.notifications import SlackTask
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefect.schedules.filters import is_weekday
from prefect.environments.storage import GitHub

import json
import requests
from datetime import timedelta


class GetStars(Task):
    def __request_body(
        self, query: str, variables: dict = None, operation_name: str = None
    ) -> dict:
        json = {"query": query}

        if variables:
            json["variables"] = variables

        if operation_name:
            json["operationName"] = operation_name

        return json

    def execute(
        self,
        query: str,
        variables: dict = None,
        operation_name: str = None,
        headers: dict = None,
    ) -> any:
        request_body = self.__request_body(
            query=query, variables=variables, operation_name=operation_name
        )

        result = requests.post(
            "https://api.github.com/graphql", json=request_body, headers=headers
        )

        result.raise_for_status()
        return result.json()

    def run(self, repository: str, owner: str) -> int:
        query = """
        query Stargazers($repository: String!, $owner: String!) {
            repository(name: $repository, owner: $owner) {
                stargazers {
                    totalCount
                }
            }
        }
        """

        variables = {"repository": repository, "owner": owner}

        token = Secret("GITHUB_AUTH_TOKEN").get()
        headers = {"Authorization": f"bearer {token}"}

        data = self.execute(query=query, variables=variables, headers=headers)
        return data["data"]["repository"]["stargazers"]["totalCount"]


class ShouldNotify(Task):
    def run(self, stars: int) -> [str, None]:
        now = prefect.context["date"]
        return stars % 1000 == 0 or (
            now.hour == 9 and now.minute >= 0 and now.minute <= 5
        )


class NotificationMessage(Task):
    def run(self, repository: str, owner: str, stars: int) -> dict:
        return {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"⭐⭐The {repository} repo has reached {stars}⭐⭐!",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "See all the new stargazers <https://github.com/{owner}/{repository}/stargazers|here>",
                    },
                },
            ]
        }


# schedule = Schedule(clocks=[IntervalClock(timedelta(minutes=5))], filters=[is_weekday])
with Flow("Stargazers") as flow:
    """
    Tasks:
        Repository [Parameter]
        Owner [Parameter]
        GetStars
        ShouldNotify
        NotificationMessage
        Notify [SlackTask]
    """
    repository = Parameter("repository", default="prefect")
    owner = Parameter("owner", default="PrefectHQ")

    stars = GetStars(name="Get Stars", max_retries=2, retry_delay=timedelta(minutes=1))(
        repository=repository, owner=owner
    )

    should_notify = ShouldNotify()(stars=stars)

    with case(should_notify, True):
        message = NotificationMessage()(repository=repository, owner=owner, stars=stars)

        notification = SlackTask(webhook_secret="STARGAZERS_SLACK_WEBHOOK_TOKEN")(
            message=message
        )

flow.storage = GitHub(
    repo="znicholasbrown/stargazers",
    path="/stargazers.flow.py",
    secrets=["GITHUB_AUTH_TOKEN"],
)


# flow.register(project_name="PROJECT: Nicholas")
flow.run()
