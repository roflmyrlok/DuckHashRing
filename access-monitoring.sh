#!/bin/bash
# access-monitoring.sh - Quick access to monitoring URLs

MINIKUBE_IP=$(minikube ip)

echo "========================================="
echo "DuckSharding Monitoring Access"
echo "========================================="
echo ""
echo "Grafana Dashboard:"
echo "  URL: http://${MINIKUBE_IP}:30300"
echo "  Username: admin"
echo "  Password: admin"
echo ""
echo "Prometheus:"
echo "  URL: http://${MINIKUBE_IP}:30090"
echo ""
echo "AlertManager:"
echo "  URL: http://${MINIKUBE_IP}:30093"
echo ""
echo "Coordinator API:"
echo "  URL: http://${MINIKUBE_IP}:30000"
echo ""
echo "RabbitMQ Management (port-forward required):"
echo "  Run: kubectl port-forward svc/rabbitmq 15672:15672"
echo "  Then visit: http://localhost:15672"
echo "  Username: guest"
echo "  Password: guest"
echo ""
