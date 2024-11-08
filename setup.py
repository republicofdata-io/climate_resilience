from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="climate_resilience",
        packages=find_packages(),
        install_requires=[
            "dagster==1.9.0",
            "dagster-cloud==1.9.0",
            "dagster-dbt==0.25.0",
            "dagster-hex==0.1.3",
            "dagster-gcp==0.25.0",
            "dagster-gcp-pandas==0.25.0",
            "dbt-bigquery==1.8.3",
            "dbt-core==1.8.8",
            "feedparser==6.0.11",
            "langchain-core==0.3.15",
            "langchain-openai==0.2.6",
            "pydantic==2.9.2",
            "spacy==3.8.2",
            "supabase==2.10.0",
        ],
    )
