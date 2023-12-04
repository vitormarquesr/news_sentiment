import os
import requests
import dotenv
from datetime import date
import pandas as pd


dotenv.load_dotenv()

API_KEY = os.environ.get("API_KEY")
CONTENT_ENDPOINT = "https://content.guardianapis.com/search"


def get_content(from_date=None, to_date=None, page=1, max_depth=None):
    # If range not specified use today's date
    from_date = date.today() if from_date is None else from_date
    to_date = date.today() if to_date is None else to_date

    params = {
        "api-key": API_KEY,
        "format": "json",
        "from-date": from_date,
        "to-date": to_date,
        "show-tags": "keyword",
        "page-size": 50,
        "page": page,
    }

    r = requests.get(CONTENT_ENDPOINT, params=params)
    response = r.json()["response"]

    if response["status"] != "ok":
        return None

    max_depth = (
        response["pages"] if max_depth is None else min([response["pages"], max_depth])
    )

    results = pd.DataFrame(response["results"])

    if page == max_depth:
        return results

    # Pagination
    return pd.concat(
        [results, get_content(from_date, to_date, page=page + 1, max_depth=max_depth)]
    )


if __name__ == "__main__":
    print(get_content())
