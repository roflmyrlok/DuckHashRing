#!/bin/bash
kubectl apply -f k8s/shards-deployment.yaml
kubectl apply -f k8s/coordinator-deployment.yaml