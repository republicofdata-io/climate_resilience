.PHONY: docker

dagster:
	dbt parse --profiles-dir climate_resilience/assets/analytics --project-dir climate_resilience/assets/analytics
	dagster dev --module-name climate_resilience

dbdocs:
	dbdocs build design/semantic_layer.dbml --project "Climate Resilience"
