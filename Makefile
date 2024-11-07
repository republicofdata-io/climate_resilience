.PHONY: docker

dagster:
	dbt parse --profiles-dir climate_resilience/assets/gold --project-dir climate_resilience/assets/gold
	dagster dev --module-name climate_resilience
