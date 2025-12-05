#!/bin/bash
# deploy.sh

eval $(minikube docker-env)

kubectl apply -f k8s/rabbitmq-deployment.yaml
kubectl wait --for=condition=ready pod -l app=rabbitmq --timeout=25s

kubectl apply -f k8s/shards-deployment.yaml
kubectl wait --for=condition=ready pod -l role=leader --timeout=25s
kubectl wait --for=condition=ready pod -l role=follower --timeout=25s

kubectl apply -f k8s/coordinator-deployment.yaml
kubectl wait --for=condition=ready pod -l app=coordinator --timeout=25s