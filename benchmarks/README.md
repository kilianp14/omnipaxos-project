# Omnipaxos-kv Benchmarks
Benchmarking code for configuring and deploying omnipaxos-kv servers and clients to [GCP](https://cloud.google.com) as docker containers. Uses the GCP python client API to provision GCP instances. Then uses gcloud for authentication and starting servers/clients via SSHing into the provisioned instances. The project is currently setup to run an example benchmark using the omnipaxos-kv omnipaxos-server and omnipaxos-client images.

Documentation on the GCP python client API seems to be scarce. The best resource I've found are the samples [here](https://github.com/GoogleCloudPlatform/python-docs-samples/tree/main/compute).
## Prerequisites
 - [gcloud](https://cloud.google.com/sdk/gcloud) CLI tool for interacting with GCP
 - [uv](https://docs.astral.sh/uv/) a Python project/package manager.
## Project Structure
 - `setup-gcp.sh` Initial GCP setup. Only necessary if you are starting a new GCP project.
 - `gcp_cluster.py` Manage cluster of GCP instances.
 - `gcp_ssh_client.py` Manage SSH connections to GCP instances.
 - `omnipaxos_cluster.py` Manage omnipaxos-kv GCP cluster.
 - `omnipaxos_configs.py` Defines configs for omnipaxos-kv.
 - `benchmarks.py` Run example benchmarks, results saved to `logs/`
 - `graph_experiment.py` Graph benchmark data in `logs/`
## Deployment steps
![gcp-diagram](https://github.com/user-attachments/assets/7dcea25f-f2f5-44a9-a15e-7c18a7e5f517)

## To Run
 1. Have an owner of the GCP project add you to the project and configure your permissions. Or start your own project with the help of `setup-gcp.sh`
 2. Copy the contents of `./scripts/project_env_template` to `./scripts/project_env.sh` and then configure environment variables in `./scripts/project_env.sh`
 3. Run the commands in `./scripts/auth.sh` to configure your gcloud credentials
 4. Run `./scripts/push-server-image.sh` and `./scripts/push-client-image.sh` to push docker images to GCP Artifact Registry
 5. Run python code with `uv run <python-file-here>`.
     - `uv run benchmarks.py` to run the example benchmark
     - `uv run graph_experiment.py` to graph benchmark data

