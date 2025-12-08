#!/bin/bash
# deploy.sh

eval $(minikube docker-env)

# Deploy RabbitMQ
kubectl apply -f k8s/rabbitmq-deployment.yaml
kubectl wait --for=condition=ready pod -l app=rabbitmq --timeout=60s

# Deploy Shards
kubectl apply -f k8s/shards-deployment.yaml
kubectl wait --for=condition=ready pod -l role=leader --timeout=60s
kubectl wait --for=condition=ready pod -l role=follower --timeout=60s

# Deploy Coordinator
kubectl apply -f k8s/coordinator-deployment.yaml
kubectl wait --for=condition=ready pod -l app=coordinator --timeout=60s

# Deploy Monitoring Stack
kubectl apply -f k8s/monitoring-deployment.yaml
kubectl wait --for=condition=ready pod -l app=prometheus --timeout=60s
kubectl wait --for=condition=ready pod -l app=grafana --timeout=60s
kubectl wait --for=condition=ready pod -l app=alertmanager --timeout=60s

echo ""
echo "========================================="
echo "Deployment Complete!"
echo "========================================="
echo ""
echo "Access the services:"
echo "  Coordinator API:  http://$(minikube ip):30000"
echo "  Prometheus:       http://$(minikube ip):30090"
echo "  Grafana:          http://$(minikube ip):30300 (admin/admin)"
echo "  AlertManager:     http://$(minikube ip):30093"
echo "  RabbitMQ UI:      kubectl port-forward svc/rabbitmq 15672:15672"
echo ""