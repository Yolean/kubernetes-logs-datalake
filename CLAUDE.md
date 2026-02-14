
## rules

- all kubectl access should use --kubeconfig=$(pwd)/k3d-example/kubeconfig
- Use `npm install` for dependencies (not yarn). The `packageManager` field is set to yarn for turborepo workspace resolution only.
- Use `jq` or `yq` to parse json/yaml, never use python
- Never use `npx`. Only use installed dependencies.
