# 📦 Systolic Array Orchestrator

This is a utility repo used to generate a Docker image that is called in the GitHub repo [Making Waves]. 

This image implements an orchestrator that interacts with systolic array processing elements used for NxN matrix multiplication. 

## 🌟 Highlights

- Uses FastAPI to provide a web API
- Uses HTTPX Async to communicate with systolic array PEs
- Implements a UI for the user to adjust the tick period, input NxN square matrices in JSON format, observe the execution of the matrix multiplication steps and collect the results 

## ℹ️ Overview

Please refer to [CurioCopia] for the specific blog and the relevant repo(s). 

### ✍️ Authors

All repos shared in [CurioCopia] are shared under Creative Commons license for others to adopt and use it as they wish.

## 🚀 Usage

So far, the only use of this repo was to generate the resultant Docker container and use it in a Kubernetes Deployment. Please refer to the eventual link repo how it is used.

## ⬇️ Installation

Build the Docker container with your preferred tool, tag and commit to your choice of registry. 

```bash
podman build -t systolic-orchestrator .
podman tag redis-reader <registry>/systolic-orchestrator
podman push <registry>/systolic-orchestrator
```

## 💭 Feedback and Contributing

Please refer to the GithUb repo [Making Waves].

[Making Waves]: https://github.com/Curiocopia/blog-making-waves
[Curiocopia]: https://curiocopia.com