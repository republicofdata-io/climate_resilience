.PHONY: docker

dagster:
	dagster dev --module-name climate_resilience_platform

dbt-compile:
	dbt parse --profiles-dir climate_resilience_platform/assets/analytics --project-dir climate_resilience_platform/assets/analytics
