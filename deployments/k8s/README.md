# goCache on Kubernetes

这个目录提供 goCache 项目的最小可运行 K8s 部署。

## 架构

- `go-cache`：StatefulSet，3 副本，每个 Pod 同时提供
  - Peer 端口 `8001`（groupcache 节点互联）
  - API 端口 `9999`（`/api` 和 `/debug/stats`）
- `redis`：单副本 Deployment，用作 getter 的后端缓存源
- `gocache`：Headless Service（给 StatefulSet 稳定 DNS）
- `gocache-api`：ClusterIP Service（对内统一访问 API）

## 1. 快速上手（kind）

1. 创建集群：

```powershell
kind create cluster --name gocache
```

2. 在仓库根目录构建镜像：

```powershell
docker build -t gocache:local .
```

3. 将镜像加载到 kind：

```powershell
kind load docker-image gocache:local --name gocache
```

4. 部署：

```powershell
kubectl apply -k ./k8s
```

5. 查看状态：

```powershell
kubectl -n gocache get pods -w
kubectl -n gocache get svc
```

6. 本地访问 API：

```powershell
kubectl -n gocache port-forward svc/gocache-api 9999:9999
curl "http://127.0.0.1:9999/api?key=Tom"
curl "http://127.0.0.1:9999/debug/stats"
```

## 2. 通用集群部署（非 kind）

如果你是远端集群（AKS/EKS/GKE/自建），请先把镜像推送到可访问仓库，然后把 `cache-statefulset.yaml` 里的镜像改成你的地址，例如：

```yaml
image: registry.example.com/team/gocache:v1
imagePullPolicy: IfNotPresent
```

然后执行：

```powershell
kubectl apply -k ./k8s
```

## 3. 常用排查

```powershell
kubectl -n gocache logs statefulset/go-cache -c server --tail=200
kubectl -n gocache describe pod go-cache-0
kubectl -n gocache get endpoints gocache
```

如果 Pod 频繁重启，优先检查：

- 镜像是否可拉取
- `redis` 是否就绪
- `-self-addr` 和 `-peers` 是否和 StatefulSet 副本数一致

## 4. 扩缩容说明

当前清单把 `-peers` 写成 3 个固定 Pod DNS，因此更适合固定 3 副本场景。若要动态扩缩容，建议后续改造为：

- 从 Endpoints 自动发现 peer
- 或在启动脚本中根据 DNS 结果生成 `-peers`
