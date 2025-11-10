#!/bin/bash
# reset.sh

set -e

kubectl delete -f k8s/coordinator-deployment.yaml --ignore-not-found=true
kubectl delete -f k8s/shards-deployment.yaml --ignore-not-found=true
kubectl delete -f k8s/rabbitmq-deployment.yaml --ignore-not-found=true

kubectl wait --for=delete pod -l app=coordinator --timeout=15s 2>/dev/null || true
kubectl wait --for=delete pod -l app=duck-shard --timeout=15s 2>/dev/null || true
kubectl wait --for=delete pod -l app=rabbitmq --timeout=15s 2>/dev/null || true

kubectl delete pvc --all --ignore-not-found=true

minikube stop

minikube delete

minikube start

eval $(minikube docker-env)

docker system prune -af --volumes