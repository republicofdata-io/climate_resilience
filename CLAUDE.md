# Climate Resilience Platform Dev Guide

## Build Commands
- `make dagster` - Run Dagster development server
- `dbt parse --profiles-dir climate_resilience/assets/analytics --project-dir climate_resilience/assets/analytics` - Parse DBT models
- `poetry install` - Install dependencies
- `poetry run python -m pytest` - Run tests (when implemented)

## Code Style Guidelines
- **Imports**: Standard library first, third-party libs second, project modules last
- **Naming**: Classes use PascalCase, functions/variables use snake_case, constants use UPPER_SNAKE_CASE
- **Types**: Use type hints extensively with Python's typing module
- **Documentation**: Classes and functions should have docstrings with descriptions, parameters, and return values
- **Error Handling**: Create custom exception classes, use explicit error checking, provide informative error messages
- **Data Processing**: Use Pandas for data manipulation with explicit type conversions
- **Format**: 4-space indentation, max line length ~88 chars, consistent spacing around operators

## Key Technologies
- **Dagster**: Data orchestration framework
- **DBT**: Data transformation tool
- **LangChain**: Framework for LLM applications
- **Pandas**: Data manipulation and analysis
- **Pydantic**: Data validation and settings management