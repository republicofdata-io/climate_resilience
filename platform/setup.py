from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="data_platform",
        packages=find_packages(),
        install_requires=[
            "dagster==1.8.2",
            "dagster-cloud==1.8.2",
            "dagster-gcp==0.24.2",
            "dagster-gcp-pandas==0.24.2",
            "feedparser==6.0.11",
        ],
    )
