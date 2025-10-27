using k8s;
using k8s.Models;
using DuckSharding.Shared.Models;

namespace DuckSharding.Coordinator;

public class ShardProcessManager
{
    private readonly Coordinator _coordinator;
    private readonly IKubernetes _k8sClient;
    private readonly string _namespace;
    private int _nextPort = 30001; // NodePort range starts at 30000

    public ShardProcessManager(Coordinator coordinator, IConfiguration configuration)
    {
        _coordinator = coordinator;
        _namespace = configuration["Kubernetes:Namespace"] ?? "default";
        
        var config = KubernetesClientConfiguration.IsInCluster() 
            ? KubernetesClientConfiguration.InClusterConfig() 
            : KubernetesClientConfiguration.BuildConfigFromConfigFile();
        
        _k8sClient = new Kubernetes(config);
    }

    public async Task<ShardInfo> StartShardAsync(string? shardId = null)
    {
        shardId ??= $"shard-{Guid.NewGuid().ToString()[..8]}";
        var port = _nextPort++;
        var dbFileName = $"shard{port}.db";
        
        // Create Deployment
        var deployment = new V1Deployment
        {
            Metadata = new V1ObjectMeta
            {
                Name = shardId,
                Labels = new Dictionary<string, string>
                {
                    ["app"] = "duck-shard",
                    ["shard-id"] = shardId
                }
            },
            Spec = new V1DeploymentSpec
            {
                Replicas = 1,
                Selector = new V1LabelSelector
                {
                    MatchLabels = new Dictionary<string, string>
                    {
                        ["app"] = "duck-shard",
                        ["shard-id"] = shardId
                    }
                },
                Template = new V1PodTemplateSpec
                {
                    Metadata = new V1ObjectMeta
                    {
                        Labels = new Dictionary<string, string>
                        {
                            ["app"] = "duck-shard",
                            ["shard-id"] = shardId
                        }
                    },
                    Spec = new V1PodSpec
                    {
                        Containers = new[]
                        {
                            new V1Container
                            {
                                Name = "shard",
                                Image = "ducksharding-shard:latest",
                                ImagePullPolicy = "IfNotPresent",
                                Ports = new[]
                                {
                                    new V1ContainerPort { ContainerPort = 8080 }
                                },
                                Env = new[]
                                {
                                    new V1EnvVar { Name = "ASPNETCORE_URLS", Value = "http://+:8080" },
                                    new V1EnvVar { Name = "Shard__ShardId", Value = shardId },
                                    new V1EnvVar { Name = "Database__FileName", Value = dbFileName }
                                }
                            }
                        }
                    }
                }
            }
        };

        await _k8sClient.AppsV1.CreateNamespacedDeploymentAsync(deployment, _namespace);

        // Create Service
        var service = new V1Service
        {
            Metadata = new V1ObjectMeta
            {
                Name = shardId,
                Labels = new Dictionary<string, string>
                {
                    ["app"] = "duck-shard",
                    ["shard-id"] = shardId
                }
            },
            Spec = new V1ServiceSpec
            {
                Type = "NodePort",
                Selector = new Dictionary<string, string>
                {
                    ["app"] = "duck-shard",
                    ["shard-id"] = shardId
                },
                Ports = new[]
                {
                    new V1ServicePort
                    {
                        Port = 8080,
                        TargetPort = 8080,
                        NodePort = port
                    }
                }
            }
        };

        await _k8sClient.CoreV1.CreateNamespacedServiceAsync(service, _namespace);

        // Wait for pod to be ready
        await Task.Delay(5000);

        var shardInfo = new ShardInfo(shardId, "localhost", port);
        _coordinator.AddShard(shardInfo);

        return shardInfo;
    }

    public async Task<bool> StopShardAsync(string shardId)
    {
        try
        {
            await _k8sClient.AppsV1.DeleteNamespacedDeploymentAsync(
                shardId, 
                _namespace);
            
            await _k8sClient.CoreV1.DeleteNamespacedServiceAsync(
                shardId, 
                _namespace);
            
            _coordinator.RemoveShard(shardId);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public async Task<IReadOnlyDictionary<string, bool>> GetShardProcessesAsync()
    {
        var deployments = await _k8sClient.AppsV1.ListNamespacedDeploymentAsync(
            _namespace, 
            labelSelector: "app=duck-shard");

        return deployments.Items.ToDictionary(
            d => d.Metadata.Name,
            d => (d.Status?.ReadyReplicas ?? 0) > 0
        );
    }
}