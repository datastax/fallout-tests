# Fallout tests with GKE


## What is Fallout
[Fallout](https://github.com/datastax/fallout) is a tool for running large scale remote-based distributed correctness, verification and performance tests for Apache Cassandra (TM) and Apache Pulsar (TM).

Fallout uses Kubernetes and the Helm Chart to deploy the services and run the tests. The size of the cluster is limited only by the available resources.
## What this repository is for
This repository enables to run performance tests on Cassandra 
databases using the [Fallout tool](https://github.com/datastax/fallout).

## Dependencies
- Have access to a [Google Kubernetes Engine (GKE)](https://cloud.google.com/kubernetes-engine) account.

## How to run the tests
The official [docker image](https://hub.docker.com/r/datastax/fallout) is used as it bundles all required dependencies, and the `fallout exec` command runs a single test definition.

Here's an example command to run a single test run locally:

```
docker run -it -v $(pwd):<USER_PATH/dir> \
  datastax/fallout:1.288.0 \
  fallout exec --use-unique-output-dir \
               --params <USER_PATH/dir>/template-params.yaml \
                        <USER_PATH/dir>/<test-yaml-file>.yaml \
                        <USER_PATH/dir>/creds.yaml \
                        <USER_PATH/dir>/test_out
```
This command assumes that you have a `creds.yaml`, `template-params.yaml`, and the `<test-yaml-file>.yaml` that you want to run in your directory.


Arguments:
```
test-yaml-file         Fallout test definition YAML file
creds-yaml-file        GKE credentials YAML file
output-dir             Where to write test run artifacts; 
                         it's an error if the directory isn't empty
template-params        Template params for test-yaml-file in the form
                         param=value
```
