import os
import requests
import dotenv
from datetime import date
import pandas as pd


dotenv.load_dotenv()

API_KEY = os.environ.get("API_KEY")
CONTENT_ENDPOINT = "https://content.guardianapis.com/search"


def extract_publications_from_api(page=1):
    params = {
        "api-key": API_KEY,
        "format": "json",
        "from-date": date.today(),
        "to-date": date.today(),
        "show-tags": "keyword",
        "page-size": 50,
        "page": page,
    }

    r = requests.get(CONTENT_ENDPOINT, params=params)
    response = r.json()["response"]

    if response["status"] != "ok":
        return None

    results = pd.DataFrame(response["results"])

    if page == response["pages"]:
        return results

    # Pagination
    return pd.concat(
        [
            results,
            extract_publications_from_api(page=page + 1),
        ],
        ignore_index=True,
    )


def transform_tags_to_model(df):
    df_tags = df[["id", "tags"]].explode("tags")

    # Publications metadata
    df_publications = df.drop(["tags"], axis=1)

    publication_id = df_tags["id"].reset_index(drop=True)
    df_keywords = pd.json_normalize(df_tags["tags"])
    df_keywords = df_keywords[["id", "sectionId", "webTitle"]]

    # Association publication - keyword
    df_tags = pd.concat([publication_id, df_keywords["id"]], axis=1)

    df_tags.columns = ["publication_id", "keyword_id"]

    # Keywords metadata
    df_keywords = df_keywords.drop_duplicates("id").reset_index(drop=True)
    df_keywords.columns = ["id", "section_id", "title"]

    return (df_publications, df_tags, df_keywords)


def transform_section_to_model(df_publications):
    df_sections = df_publications[["sectionId", "sectionName"]]
    df_sections = df_sections.drop_duplicates("sectionId")
    df_sections.columns = ["id", "title"]

    df_publications = df_publications.drop("sectionName", axis=1)

    return (df_publications, df_sections)


def transform_pillar_to_model(df_publications):
    df_pillars = df_publications[["pillarId", "pillarName"]]
    df_pillars = df_pillars.drop_duplicates("pillarId")
    df_pillars.columns = ["id", "title"]

    df_publications = df_publications.drop("pillarName", axis=1)

    return (df_publications, df_pillars)


def transform_clean_publications(df_publications):
    df_publications = df_publications.drop("isHosted", axis=1)
    print(df_publications.info())
    df_publications.columns = [
        "id",
        "type",
        "section_id",
        "publication_date",
        "title",
        "web_url",
        "api_url",
        "pillar_id",
    ]
    return df_publications


def etl():
    df = extract_publications_from_api()
    df_publications, df_tags, df_keywords = transform_tags_to_model(df)
    df_publications, df_sections = transform_section_to_model(df_publications)
    df_publications, df_pillars = transform_pillar_to_model(df_publications)
    df_publications = transform_clean_publications(df_publications)

    return (df_publications, df_tags, df_keywords, df_sections, df_pillars)