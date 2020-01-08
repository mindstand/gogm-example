# gogm-example
Example Project for GoGM

## Description
This is based on the loosely Java example for the Java OGM in the [neo4j documentation](https://neo4j.com/docs/ogm-manual/current/tutorial/).

## Files
- `docker-compose.yaml` - deploys single node neo4j "cluster"
- `docker-compose-casual-cluster.yaml` - deploys 3 core and 1 replica node cluster
- `go.mod`/`go.sum` - required for go modules
- `models.go` - contains models for the example
- `linking.go` - generated by gogmcli for node linking and unlinking
- `main.go` - gogm usage example