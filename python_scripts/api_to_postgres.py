import os
import requests
import dotenv
from datetime import date
import pandas as pd
import psycopg2


dotenv.load_dotenv()

API_KEY = os.environ.get("API_KEY")
CONTENT_ENDPOINT = "https://content.guardianapis.com/search"
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_DATABASE = os.environ.get("POSTGRES_DATABASE")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

conn = psycopg2.connect(
    host=POSTGRES_HOST,
    database=POSTGRES_DATABASE,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    port=POSTGRES_PORT,
)
cur = conn.cursor()


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

    df_keywords = df_keywords[["id", "sectionId", "sectionName", "webTitle"]]

    # Association publication - keyword
    df_tags = pd.concat([publication_id, df_keywords["id"]], axis=1)

    df_tags.columns = ["publication_id", "keyword_id"]

    # Keywords metadata
    df_keywords = df_keywords.drop_duplicates("id").reset_index(drop=True)
    df_keywords.columns = ["id", "sectionId", "sectionName", "title"]

    return (df_publications, df_tags, df_keywords)


def transform_section_to_model(df_publications, df_keywords):
    df_sections = df_publications[["sectionId", "sectionName"]]
    keyword_sections = df_keywords[["sectionId", "sectionName"]]

    df_sections = pd.concat([df_sections, keyword_sections], ignore_index=True)

    df_keywords = df_keywords.drop(["sectionName"], axis=1)
    df_keywords.columns = ["id", "section_id", "title"]

    df_sections = df_sections.drop_duplicates("sectionId")
    df_sections.columns = ["id", "title"]

    df_publications = df_publications.drop("sectionName", axis=1)

    return (df_publications, df_sections, df_keywords)


def transform_pillar_to_model(df_publications):
    df_pillars = df_publications[["pillarId", "pillarName"]]
    df_pillars = df_pillars.drop_duplicates("pillarId")
    df_pillars.columns = ["id", "title"]

    df_publications = df_publications.drop("pillarName", axis=1)

    return (df_publications, df_pillars)


def transform_clean_publications(df_publications):
    df_publications = df_publications.drop("isHosted", axis=1)
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


def load_create_tables():
    # Sections
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS sections(
        id VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255)
    );"""
    )

    # Pillars
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS pillars(
        id VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255)
    );"""
    )
    # Keywords
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS keywords(
        id VARCHAR(255) PRIMARY KEY,
        section_id VARCHAR(255) REFERENCES sections(id),
        title VARCHAR(255)
    );"""
    )

    # Publications

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS publications(
        id VARCHAR(255) PRIMARY KEY,
        type VARCHAR(255),
        section_id VARCHAR(255) REFERENCES sections(id),
        publication_date TIMESTAMP,
        title VARCHAR(255),
        web_url VARCHAR(255),
        api_url VARCHAR(255),
        pillar_id VARCHAR(255) REFERENCES pillars(id)
    );
        """
    )

    # Tags

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS tags(
        publication_id VARCHAR(255) REFERENCES publications(id),
        keyword_id VARCHAR(255) REFERENCES keywords(id),
        PRIMARY KEY(publication_id, keyword_id)
    );"""
    )


def load_sections(df_sections):
    for _, row in df_sections.iterrows():
        cur.execute(
            """INSERT INTO sections(id, title) VALUES (%s, %s)
            ON CONFLICT DO NOTHING""",
            (row.iloc[0], row.iloc[1]),
        )


def load_pillars(df_pillars):
    for _, row in df_pillars.iterrows():
        cur.execute(
            """INSERT INTO pillars(id, title) VALUES (%s, %s)
            ON CONFLICT DO NOTHING""",
            (row.iloc[0], row.iloc[1]),
        )


def load_keywords(df_keywords):
    for _, row in df_keywords.iterrows():
        cur.execute(
            """INSERT INTO keywords(id, section_id, title) VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING""",
            (row.iloc[0], row.iloc[1], row.iloc[2]),
        )


def load_publications(df_publications):
    for _, row in df_publications.iterrows():
        cur.execute(
            """INSERT INTO publications(id, type, section_id, publication_date,
            title, web_url, api_url, pillar_id) VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
            (
                row.iloc[0],
                row.iloc[1],
                row.iloc[2],
                row.iloc[3],
                row.iloc[4],
                row.iloc[5],
                row.iloc[6],
                row.iloc[7],
            ),
        )


def load_tags(df_tags):
    for _, row in df_tags.iterrows():
        cur.execute(
            """INSERT INTO tags(publication_id, keyword_id) VALUES
            (%s, %s) ON CONFLICT DO NOTHING""",
            (row.iloc[0], row.iloc[1]),
        )


def etl():
    df = extract_publications_from_api()
    df_publications, df_tags, df_keywords = transform_tags_to_model(df)
    df_publications, df_sections, df_keywords = transform_section_to_model(
        df_publications, df_keywords
    )
    df_publications, df_pillars = transform_pillar_to_model(df_publications)
    df_publications = transform_clean_publications(df_publications)

    load_create_tables()
    load_sections(df_sections)
    load_pillars(df_pillars)
    load_keywords(df_keywords)
    load_publications(df_publications)
    load_tags(df_tags)

    conn.commit()

    return (df_publications, df_tags, df_keywords, df_sections, df_pillars)


if __name__ == "__main__":
    df_publications, df_tags, df_keywords, df_sections, df_pillars = etl()
