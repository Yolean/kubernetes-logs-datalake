
## rules

- all kubectl access should use --kubeconfig=$(pwd)/k3d-example/kubeconfig
- Use `npm install` for dependencies (not yarn). The `packageManager` field is set to yarn for turborepo workspace resolution only.
