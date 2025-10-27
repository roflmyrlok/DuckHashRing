#!/bin/bash
docker build -t ducksharding-coordinator:latest -f DuckSharding.Coordinator/Dockerfile .
docker build -t ducksharding-shard:latest -f DuckSharding.Shard/Dockerfile .
