from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="climate_resilience",
        packages=find_packages(),
        install_requires=[
            "dagster==1.9.3",
            "dagster-cloud==1.9.3",
            "dagster-dbt==0.25.3",
            "dagster-gcp==0.25.3",
            "dagster-gcp-pandas==0.25.3",
            "dbt-bigquery==1.8.3",
            "dbt-core==1.8.8",
            "duckduckgo-search==6.3.7",
            "feedparser==6.0.11",
            "langchain-community==0.3.9",
            "langchain-core==0.3.21",
            "langchain-openai==0.2.11",
            "langgraph==0.2.56",
            "pandas==2.2.3",
            "pydantic==2.9.2",
            "spacy==3.7.5",
            "supabase==2.10.0",
            "thinc==8.2.5",
            "wikipedia==1.4.0",
        ],
    )
