from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="climate_resilience_platform",
        packages=find_packages(),
        install_requires=[
            "dagster==1.8.4",
            "dagster-cloud==1.8.4",
            "dagster-gcp==0.24.4",
            "dagster-gcp-pandas==0.24.4",
            "feedparser==6.0.11",
            "langchain-core==0.2.38",
            "langchain-openai==0.1.23",
            "pydantic==2.8.2",
            "spacy==3.7.5",
            "supabase==2.5.3",
        ],
    )
