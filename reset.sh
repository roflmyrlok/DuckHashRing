#!/bin/bash
set -e

kubectl delete -f k8s/coordinator-deployment.yaml --ignore-not-found=true
kubectl delete -f k8s/shards-deployment.yaml --ignore-not-found=true
kubectl delete statefulset shard --ignore-not-found=true
kubectl delete service shard --ignore-not-found=true

kubectl wait --for=delete pod -l app=coordinator --timeout=60s 2>/dev/null || true
kubectl wait --for=delete pod -l app=duck-shard --timeout=60s 2>/dev/null || true

kubectl delete pvc -l app=duck-shard --ignore-not-found=true

eval $(minikube docker-env)

docker ps -a | grep ducksharding | awk '{print $1}' | xargs -r docker rm -f 2>/dev/null || true

docker rmi -f ducksharding-coordinator:latest 2>/dev/null || true
docker rmi -f ducksharding-shard:latest 2>/dev/null || true

kubectl get pods
kubectl get services