import prefect
from prefect import Flow, Task, Parameter, case
from prefect.tasks.notifications import SlackTask
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefect.schedules.filters import is_weekday
from python_graphql_client import GraphqlClient


from datetime import timedelta


class GetStars(Task):
    def run(self, repository: str, owner: str) -> int:
        query = """
        query Stargazers($repository: String, $owner: String) {
            repository(name: $repository, owner: $owner) {
                stargazers {
                    totalCount
                }
            }
        }
        """

        variables = {"repository": repository, "owner": owner}
        client = GraphqlClient(endpoint="https://api.github.com/graphql")

        data = client.execute(query=query, variables=variables)
        return data["data"]["repository"]["stargazers"]["totalCount"]


class ShouldNotify(Task):
    def run(self, stars: int) -> [str, None]:
        now = prefect.context["date"]
        return stars % 1000 == 0 or (
            now.hour == 9 and now.minute > 0 and now.minute < 5
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


schedule = Schedule(clocks=[IntervalClock(timedelta(minutes=5))], filters=[is_weekday])
with Flow("Stargazers", schedule=schedule) as flow:
    """
    Tasks:
        Repository [Parameter]
        Owner [Parameter]
        GetStars
        ShouldNotify
        NotificationMessage
        Notify [SlackTask]
    """
    repository = Parameter("repository", default="prefect", required=True)
    owner = Parameter("owner", default="PrefectHQ", required=True)

    stars = GetStars(name="Get Stars", max_retries=2, retry_delay=timedelta(minutes=1))(
        repository=repository, owner=owner
    )

    should_notify = ShouldNotify()(stars=stars)

    with case(should_notify, True):
        message = NotificationMessage()(repository=repository, owner=owner, stars=stars)

        notification = SlackTask(webhook_secret="STARGAZERS_SLACK_WEBHOOK_TOKEN")(
            message=message
        )
