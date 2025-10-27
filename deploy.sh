#!/bin/bash
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/coordinator-deployment.yaml
