from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="climate_resilience_platform",
        packages=find_packages(),
        install_requires=[
            "dagster==1.8.2",
            "dagster-cloud==1.8.2",
            "dagster-gcp==0.24.2",
            "dagster-gcp-pandas==0.24.2",
            "feedparser==6.0.11",
            "lanchain-core==0.2.38",
            "langchain-openai==0.1.23",
            "spacy==3.7.5",
            "supabase==2.5.3",
        ],
    )
