.PHONY: docker

dagster:
	dagster dev --module-name climate_resilience

dbt-compile:
	dbt parse --profiles-dir climate_resilience/assets/gold --project-dir climate_resilience/assets/gold
