# BGC Argo+

This repo contains database build scripts for the BGC Argo+ product in Argovis point schema format.

Usage:

- Place or mount BGC Argo+ float files in /tmp/bap
- See https://github.com/argovis/db-schema/blob/main/bgcargoplus.py for a schema to set up
- `pod.yaml` gives an example spec of how to run the build; `bash populate.sh` will do so with data in the correct place.
