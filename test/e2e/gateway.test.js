import { describe, it, expect, beforeAll } from 'vitest';
import { execSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import { resolve } from 'node:path';

const KUBECONFIG = resolve(import.meta.dirname, '../../k3d-example/kubeconfig');
const REPO_DIR = resolve(import.meta.dirname, '../..');
const CLUSTER_NAME = 'fluentbit-demo';
const GATEWAY_URL = 'http://localhost:30080';

const labelViewName = 'lakeview.yolean.se/view-name';

function kubectl(args, timeout = 60_000) {
  return execSync(`kubectl --kubeconfig=${KUBECONFIG} ${args}`, {
    encoding: 'utf-8',
    timeout,
  }).trim();
}

function importImage(tarPath) {
  const markerPath = tarPath + '.imported';
  const tarHash = execSync(`shasum -a 256 ${tarPath}`, { encoding: 'utf-8' }).split(' ')[0];
  const kcHash = execSync(`shasum -a 256 ${KUBECONFIG}`, { encoding: 'utf-8' }).split(' ')[0];
  const markerValue = `${tarHash}:${kcHash}`;

  if (existsSync(markerPath) && readFileSync(markerPath, 'utf-8').trim() === markerValue) {
    console.log(`Skipping import (unchanged): ${tarPath}`);
    return;
  }

  console.log(`Importing image: ${tarPath}`);
  execSync(`k3d image import -c ${CLUSTER_NAME} ${tarPath}`, {
    encoding: 'utf-8',
    timeout: 60_000,
  });
  writeFileSync(markerPath, markerValue);
  console.log(`Imported and marked: ${tarHash.slice(0, 12)}`);
}

async function waitForGateway(maxAttempts = 10) {
  for (let i = 1; i <= maxAttempts; i++) {
    try {
      const res = await fetch(`${GATEWAY_URL}/`);
      if (res.ok) {
        // Also verify /_api/ routes work (sidecar reachable through envoy)
        const apiRes = await fetch(`${GATEWAY_URL}/_api/views`);
        if (apiRes.ok) {
          console.log(`Gateway responsive after ${i} attempt(s)`);
          return;
        }
      }
    } catch { /* not ready yet */ }
    if (i === maxAttempts) throw new Error('Gateway not responsive after ' + maxAttempts + ' attempts');
    await new Promise(r => setTimeout(r, 1000));
  }
}

// Snapshot services that exist before gateway restart
let preExistingViewNames = [];

describe('gateway', () => {
  beforeAll(async () => {
    importImage(resolve(REPO_DIR, 'images/gateway-sidecar/target/images/gateway-sidecar.tar'));
    importImage(resolve(REPO_DIR, 'images/duckdb/target/images/duckdb-ui.tar'));

    console.log('Ensuring ui namespace exists');
    try { kubectl('create namespace ui'); } catch { /* already exists */ }

    // Clean up leftover test resources that could cause stale endpoints
    for (const name of ['test01']) {
      try { kubectl(`delete job view-${name} -n ui --ignore-not-found`); } catch { /* ok */ }
      try { kubectl(`delete service view-${name} -n ui --ignore-not-found`); } catch { /* ok */ }
    }
    // Wait for pods from deleted jobs to terminate
    try {
      kubectl(`wait --for=delete pod -l ${labelViewName}=test01 -n ui --timeout=30s`, 40_000);
    } catch { /* no pods or already gone */ }

    // Snapshot which lakeview services exist before restart
    const svcJson = kubectl(`get services -n ui -l app=lakeview -o json`);
    const svcs = JSON.parse(svcJson).items;
    preExistingViewNames = svcs.map(s => s.metadata.labels[labelViewName]).filter(Boolean);
    console.log(`Pre-existing view services: ${preExistingViewNames.length > 0 ? preExistingViewNames.join(', ') : '(none)'}`);

    console.log('Applying S3 credentials secret');
    kubectl(`apply -k ${resolve(REPO_DIR, 'k3d-example/ui')}`);

    console.log('Applying gateway kustomize');
    kubectl(`apply -k ${resolve(REPO_DIR, 'ui/gateway')}`);

    console.log('Restarting gateway deployment');
    kubectl('rollout restart deployment/gateway -n ui');

    console.log('Waiting for rollout');
    kubectl('rollout status deployment/gateway -n ui --timeout=60s');

    console.log('Waiting for gateway to be responsive');
    await waitForGateway();
  }, 120_000);

  it('responds with "gateway ok"', async () => {
    const res = await fetch(`${GATEWAY_URL}/`);
    expect(res.ok).toBe(true);
    const text = await res.text();
    expect(text).toContain('gateway ok');
  }, 60_000);

  describe('pre-existing services survive restart', () => {
    it('all pre-existing lakeview services are listed after restart', async () => {
      if (preExistingViewNames.length === 0) {
        console.log('No pre-existing services to verify (create a view via the README workflow first)');
        return;
      }

      let listedNames = [];
      for (let attempt = 1; attempt <= 10; attempt++) {
        const res = await fetch(`${GATEWAY_URL}/_api/views`);
        const views = await res.json();
        listedNames = views.map(v => v.name);
        if (preExistingViewNames.every(name => listedNames.includes(name))) break;
        console.log(`Attempt ${attempt}: listed=${listedNames.join(',')} expected=${preExistingViewNames.join(',')}`);
        if (attempt < 10) await new Promise(r => setTimeout(r, 1000));
      }

      for (const name of preExistingViewNames) {
        expect(listedNames, `pre-existing view "${name}" not discovered after restart`).toContain(name);
      }
    }, 30_000);
  });

  describe('view lifecycle', () => {
    const viewName = 'test01';

    it('POST /_api/views creates a view', async () => {
      const res = await fetch(`${GATEWAY_URL}/_api/views`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: viewName, subset: { cluster: 'dev' } }),
      });
      expect(res.ok).toBe(true);
      const body = await res.json();
      expect(body.name).toBe(viewName);
    }, 60_000);

    it('GET /_api/views lists the view', async () => {
      let views;
      for (let attempt = 1; attempt <= 5; attempt++) {
        const res = await fetch(`${GATEWAY_URL}/_api/views`);
        views = await res.json();
        if (views.some(v => v.name === viewName)) break;
        if (attempt < 5) await new Promise(r => setTimeout(r, 1000));
      }
      expect(views.some(v => v.name === viewName)).toBe(true);
    }, 60_000);

    it('cold-start: first request triggers Job creation and routes when ready', async () => {
      // ext_authz handles cold start: creates Job, waits for ready endpoints, then routes.
      // Node.js fetch doesn't reliably override the Host header, so use http.request.
      const text = await new Promise((resolve, reject) => {
        const req = http.request({
          hostname: 'localhost',
          port: 30080,
          path: '/',
          method: 'GET',
          headers: { Host: `${viewName}.localhost` },
          timeout: 90_000,
        }, (res) => {
          let body = '';
          res.on('data', chunk => body += chunk);
          res.on('end', () => resolve(body));
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('request timed out')); });
        req.end();
      });
      console.log(`Cold-start response (first 200 chars): ${text.substring(0, 200)}`);
      expect(text).toContain('hatchling');
    }, 120_000);

    it('DELETE /_api/views/:name removes the view', async () => {
      const res = await fetch(`${GATEWAY_URL}/_api/views/${viewName}`, { method: 'DELETE' });
      expect(res.ok).toBe(true);
      const body = await res.json();
      expect(body.status).toBe('deleted');

      // Verify job is gone
      const jobs = kubectl(`get jobs -n ui -l ${labelViewName}=${viewName} -o json`);
      const jobList = JSON.parse(jobs);
      expect(jobList.items).toHaveLength(0);
    }, 60_000);
  });
});
