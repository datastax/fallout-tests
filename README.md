# Fallout tests with GKE

## What this repository is for
This repository can be used to:
- either run performance tests for Cassandra 
databases using the [Fallout tool](https://github.com/datastax/fallout).
- or create the csv file of results from performance tests that will be fed to [hunter](https://github.com/datastax-labs/hunter).

Instructions for each of the two purposes above are provided below.


## Running performance tests via Fallout on Cassandra

### What is Fallout
[Fallout](https://github.com/datastax/fallout) is a tool for running large scale remote-based distributed correctness, verification and performance tests for Apache Cassandra (TM) and Apache Pulsar (TM).

Fallout uses Kubernetes and the Helm Chart to deploy the services and run the tests. The size of the cluster is limited only by the available resources.

### Dependencies to run performance tests on Cassandra
- Have access to a [Google Kubernetes Engine (GKE)](https://cloud.google.com/kubernetes-engine) account.

### How to run the performance tests for Cassandra
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


## Analysing performance test results with hunter

### What is Hunter
[Hunter](https://github.com/datastax-labs/hunter) performs statistical analysis of performance test results stored in CSV files or Graphite database. 
It finds change-points and notifies about possible performance regressions.

### Prospective and retrospective analyses
The Python script `src/scripts/hunter_csv/create_hunter_csv.py` can be run in two ways based on the constant `PROSPECTIVE_MODE` set in the file `src/scripts/constants.py`: 
- if `False`, a retrospective analysis would be carried out to generate one df and corresponding csv file for each test type that groups performance results already exported on a nightly basis from previous dates;
- if `True` (by default), a prospective analysis would be performed wherein only the current/ latest test run's results are taken and concatenated to the above results from the 
retrospective analysis on a nightly basis.

In future, this constant would be an input argument of a command line tool for ease of usage.

### Dependencies to create the csv file for hunter
- [Install hunter](https://github.com/datastax-labs/hunter#installation)
- Run `conda env create --file environment.yml` to install required dependencies for codes under `src` and `tests` folders

### How to generate a csv for Hunter
- Run `python src/scripts/hunter_csv/create_hunter_csv.py`
- 6 csv files will be output in the top-level directory, which can be fed to Hunter (one at a time for each type of test)

### How to send an email report with performance regressions
- Run `python src/scripts/cassandra_email/create_send_email.py`

### Linting the codes to create a csv for Hunter
- Run `isort src` to ensure a correct and consistent order of imports across all `.py` files
- Run `cd src` and then execute `autopep8 --in-place --recursive .` to ensure adherence to PEP-8 Python coding and styling standards
