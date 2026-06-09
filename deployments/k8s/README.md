# cachecore on Kubernetes

This directory contains a minimal Kubernetes deployment for the cachecore demo
server.

## Architecture

- `go-cache`: a 3-replica StatefulSet. Each pod exposes:
  - peer port `8001` for cache node traffic
  - API port `9999` for `/api` and `/debug/stats`
- `redis`: a single-replica Deployment used as the demo backend.
- `gocache`: a headless Service for stable StatefulSet DNS.
- `gocache-api`: a ClusterIP Service for API access inside the cluster.

## Quick Start with kind

Create the cluster:

```powershell
kind create cluster --name gocache
```

Build the image from the repository root:

```powershell
docker build -t gocache:local .
```

Load the image into kind:

```powershell
kind load docker-image gocache:local --name gocache
```

Deploy:

```powershell
kubectl apply -k ./deployments/k8s
```

Check status:

```powershell
kubectl -n gocache get pods -w
kubectl -n gocache get svc
```

Forward the API locally:

```powershell
kubectl -n gocache port-forward svc/gocache-api 9999:9999
curl "http://127.0.0.1:9999/api?key=Tom"
curl "http://127.0.0.1:9999/debug/stats"
```

## Remote Clusters

For AKS, EKS, GKE, or self-hosted clusters, push the image to a registry first,
then update `cache-statefulset.yaml`:

```yaml
image: registry.example.com/team/cachecore:v1
imagePullPolicy: IfNotPresent
```

Apply the manifests:

```powershell
kubectl apply -k ./deployments/k8s
```

## Troubleshooting

```powershell
kubectl -n gocache logs statefulset/go-cache -c server --tail=200
kubectl -n gocache describe pod go-cache-0
kubectl -n gocache get endpoints gocache
```

If pods restart frequently, check:

- whether the image can be pulled
- whether `redis` is ready
- whether `-self-addr` and `-peers` match the StatefulSet replica count

## Scaling

The current manifests hard-code `-peers` for three pod DNS names, so they are
best suited to a fixed 3-replica deployment. For dynamic scaling, add endpoint
discovery or generate `-peers` from DNS during startup.
