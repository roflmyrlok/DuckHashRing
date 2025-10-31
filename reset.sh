#!/bin/bash
set -e

kubectl delete -f k8s/shards-deployment.yaml
kubectl delete -f k8s/coordinator-deployment.yaml

eval $(minikube docker-env)
docker rmi ducksharding-coordinator:latest
docker rmi ducksharding-shard:latest

kubectl get pods
kubectl get services