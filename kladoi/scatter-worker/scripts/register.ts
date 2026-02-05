#!/usr/bin/env npx tsx
/**
 * Klados Registration Script
 *
 * Fully automated registration flow:
 * 1. Create collection (if needed)
 * 2. Create klados (status: development)
 * 3. Request verification token
 * 4. Push verification secrets to Cloudflare
 * 5. Deploy worker
 * 6. Wait for deployment
 * 7. Confirm verification
 * 8. Activate klados
 * 9. Create and push API key
 * 10. Cleanup verification secrets
 *
 * Usage:
 *   ARKE_USER_KEY=uk_... npx tsx scripts/register.ts          # Test network
 *   ARKE_USER_KEY=uk_... npx tsx scripts/register.ts --production  # Main network
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { execSync } from 'child_process';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';

interface AgentConfig {
  label: string;
  description: string;
  endpoint: string;
  actions_required: string[];
  accepts: { types: string[]; cardinality: 'one' | 'many' };
  produces: { types: string[]; cardinality: 'one' | 'many' };
}

interface AgentState {
  klados_id: string;
  collection_id: string;
  api_key_prefix: string;
  endpoint: string;
  endpoint_verified_at?: string;
  registered_at: string;
  updated_at: string;
}

type Network = 'test' | 'main';

// =============================================================================
// API Functions
// =============================================================================

async function apiRequest<T>(
  network: Network,
  method: string,
  path: string,
  body?: unknown
): Promise<T> {
  const res = await fetch(`${ARKE_API_BASE}${path}`, {
    method,
    headers: {
      'Authorization': `ApiKey ${ARKE_USER_KEY}`,
      'Content-Type': 'application/json',
      'X-Arke-Network': network,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!res.ok) {
    const error = await res.text();
    throw new Error(`API error (${res.status}): ${error}`);
  }

  return res.json() as Promise<T>;
}

// =============================================================================
// Wrangler Functions
// =============================================================================

function pushSecret(name: string, value: string): boolean {
  try {
    execSync(`echo "${value}" | wrangler secret put ${name}`, {
      stdio: 'pipe',
    });
    return true;
  } catch {
    return false;
  }
}

function deleteSecret(name: string): boolean {
  try {
    execSync(`wrangler secret delete ${name} --force`, {
      stdio: 'pipe',
    });
    return true;
  } catch {
    return false;
  }
}

function deployWorker(): boolean {
  try {
    execSync('wrangler deploy', {
      stdio: 'inherit',
    });
    return true;
  } catch {
    return false;
  }
}

function updateWranglerConfig(kladosId: string): boolean {
  try {
    const wranglerPath = 'wrangler.jsonc';
    if (!existsSync(wranglerPath)) return false;

    let content = readFileSync(wranglerPath, 'utf-8');
    // Replace AGENT_ID placeholder or existing value
    content = content.replace(
      /"AGENT_ID":\s*"[^"]*"/,
      `"AGENT_ID": "${kladosId}"`
    );
    writeFileSync(wranglerPath, content);
    return true;
  } catch {
    return false;
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

async function waitForDeployment(endpoint: string, maxWaitMs = 30000): Promise<boolean> {
  const startTime = Date.now();
  const checkInterval = 2000;

  console.log(`  Waiting for ${endpoint}/health...`);

  while (Date.now() - startTime < maxWaitMs) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000);

      const res = await fetch(`${endpoint}/health`, {
        method: 'GET',
        signal: controller.signal,
      });

      clearTimeout(timeoutId);
      if (res.ok) return true;
    } catch {
      // Ignore errors, keep trying
    }
    await new Promise(resolve => setTimeout(resolve, checkInterval));
  }
  return false;
}

function getKeyPrefix(key: string): string {
  return key.slice(0, 10) + '...';
}

// =============================================================================
// Registration Functions
// =============================================================================

async function getOrCreateCollection(
  network: Network,
  stateFile: string
): Promise<string> {
  // Check for existing state
  if (existsSync(stateFile)) {
    const state: AgentState = JSON.parse(readFileSync(stateFile, 'utf-8'));
    if (state.collection_id) {
      console.log(`  Using existing collection: ${state.collection_id}`);
      return state.collection_id;
    }
  }

  console.log(`  Creating collection...`);
  const { id } = await apiRequest<{ id: string }>(
    network,
    'POST',
    '/collections',
    {
      label: 'Klados Agents',
      description: 'Collection for klados worker agents',
      roles: {
        public: ['*:view', '*:invoke'],
        viewer: ['*:view'],
        editor: ['*:view', '*:update', '*:create', '*:invoke'],
        owner: ['*:view', '*:update', '*:create', '*:manage', '*:invoke', 'collection:update', 'collection:manage'],
      },
    }
  );
  console.log(`  Created collection: ${id}`);
  return id;
}

async function createKlados(
  network: Network,
  config: AgentConfig,
  collectionId: string
): Promise<{ id: string; cid: string }> {
  console.log(`  Creating klados...`);
  const result = await apiRequest<{ id: string; cid: string }>(
    network,
    'POST',
    '/kladoi',
    {
      label: config.label,
      description: config.description,
      endpoint: config.endpoint,
      actions_required: config.actions_required,
      accepts: config.accepts,
      produces: config.produces,
      collection: collectionId,
    }
  );
  console.log(`  Created klados: ${result.id} (status: development)`);
  return result;
}

async function requestVerificationToken(
  network: Network,
  kladosId: string
): Promise<{ token: string; expiresAt: string }> {
  console.log(`  Requesting verification token...`);
  const result = await apiRequest<{ verification_token: string; expires_at: string }>(
    network,
    'POST',
    `/kladoi/${kladosId}/verify`,
    {}
  );
  console.log(`  Token generated (expires: ${result.expires_at})`);
  return { token: result.verification_token, expiresAt: result.expires_at };
}

async function confirmVerification(
  network: Network,
  kladosId: string
): Promise<{ verified: boolean; error?: string; verifiedAt?: string }> {
  console.log(`  Confirming verification...`);
  const result = await apiRequest<{ verified: boolean; error?: string; verified_at?: string; message?: string }>(
    network,
    'POST',
    `/kladoi/${kladosId}/verify`,
    { confirm: true }
  );

  if (result.verified) {
    console.log(`  Endpoint verified!`);
    return { verified: true, verifiedAt: result.verified_at };
  } else {
    return { verified: false, error: result.message || result.error };
  }
}

async function activateKlados(network: Network, kladosId: string): Promise<void> {
  console.log(`  Activating klados...`);
  const { cid } = await apiRequest<{ cid: string }>(
    network,
    'GET',
    `/entities/${kladosId}/tip`
  );

  await apiRequest(
    network,
    'PUT',
    `/kladoi/${kladosId}`,
    {
      expect_tip: cid,
      status: 'active',
    }
  );
  console.log(`  Klados activated!`);
}

async function createKladosApiKey(
  network: Network,
  kladosId: string,
  label: string
): Promise<{ key: string; prefix: string }> {
  console.log(`  Creating klados API key...`);
  const result = await apiRequest<{ key: string; prefix: string }>(
    network,
    'POST',
    `/kladoi/${kladosId}/keys`,
    { label }
  );
  console.log(`  Klados API key created: ${result.prefix}...`);
  return result;
}

async function updateKlados(
  network: Network,
  kladosId: string,
  config: AgentConfig
): Promise<void> {
  console.log(`  Updating klados...`);
  const { cid } = await apiRequest<{ cid: string }>(
    network,
    'GET',
    `/entities/${kladosId}/tip`
  );

  await apiRequest(
    network,
    'PUT',
    `/kladoi/${kladosId}`,
    {
      expect_tip: cid,
      label: config.label,
      description: config.description,
      endpoint: config.endpoint,
      actions_required: config.actions_required,
      accepts: config.accepts,
      produces: config.produces,
    }
  );
  console.log(`  Klados updated`);
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  if (!ARKE_USER_KEY) {
    console.error('Error: ARKE_USER_KEY environment variable is required');
    process.exit(1);
  }

  const isProduction = process.argv.includes('--production') || process.argv.includes('--prod');
  const network: Network = isProduction ? 'main' : 'test';
  const stateFile = isProduction ? '.klados-state.prod.json' : '.klados-state.json';

  console.log(`\n📦 Klados Registration (${network} network)\n`);

  if (!existsSync('agent.json')) {
    console.error('Error: agent.json not found');
    process.exit(1);
  }

  const config: AgentConfig = JSON.parse(readFileSync('agent.json', 'utf-8'));
  console.log(`Agent: ${config.label}`);
  console.log(`Endpoint: ${config.endpoint}`);
  console.log('');

  try {
    // Check for existing state
    let existingState: AgentState | null = null;
    if (existsSync(stateFile)) {
      existingState = JSON.parse(readFileSync(stateFile, 'utf-8'));
    }

    if (existingState?.klados_id) {
      // =========================================================================
      // Update existing klados
      // =========================================================================
      console.log(`Found existing klados: ${existingState.klados_id}`);

      const endpointChanged = existingState.endpoint !== config.endpoint;

      await updateKlados(network, existingState.klados_id, config);

      if (endpointChanged) {
        console.log(`\n⚠️  Endpoint changed, re-verification required`);
        console.log(`   Old: ${existingState.endpoint}`);
        console.log(`   New: ${config.endpoint}`);

        // Re-verification flow
        const verification = await requestVerificationToken(network, existingState.klados_id);

        console.log(`\n🔐 Pushing verification secrets...`);
        pushSecret('VERIFICATION_TOKEN', verification.token);
        pushSecret('ARKE_VERIFY_AGENT_ID', existingState.klados_id);

        console.log(`\n🚀 Deploying worker...`);
        deployWorker();

        console.log(`\n⏳ Waiting for deployment...`);
        const isReady = await waitForDeployment(config.endpoint);
        if (!isReady) {
          console.warn('  ⚠️  Health check timed out, attempting verification anyway...');
        }

        const verifyResult = await confirmVerification(network, existingState.klados_id);
        if (!verifyResult.verified) {
          throw new Error(`Verification failed: ${verifyResult.error}`);
        }

        await activateKlados(network, existingState.klados_id);

        // Cleanup verification secrets
        console.log(`\n🧹 Cleaning up verification secrets...`);
        deleteSecret('VERIFICATION_TOKEN');
        deleteSecret('ARKE_VERIFY_AGENT_ID');

        // Update state
        existingState.endpoint = config.endpoint;
        existingState.endpoint_verified_at = verifyResult.verifiedAt;
        existingState.updated_at = new Date().toISOString();
        writeFileSync(stateFile, JSON.stringify(existingState, null, 2));
      } else {
        existingState.updated_at = new Date().toISOString();
        writeFileSync(stateFile, JSON.stringify(existingState, null, 2));
      }

      console.log(`\n✅ Klados updated successfully!`);
      console.log(`   ID: ${existingState.klados_id}`);

    } else {
      // =========================================================================
      // Create new klados
      // =========================================================================
      console.log('Creating new klados...\n');

      // Step 1: Get or create collection
      const collectionId = await getOrCreateCollection(network, stateFile);

      // Step 2: Create klados
      const klados = await createKlados(network, config, collectionId);

      // Step 3: Request verification token
      console.log(`\n🔐 Endpoint Verification`);
      const verification = await requestVerificationToken(network, klados.id);

      // Step 4: Push verification secrets
      console.log(`\n📤 Pushing verification secrets to Cloudflare...`);
      if (!pushSecret('VERIFICATION_TOKEN', verification.token)) {
        throw new Error('Failed to push VERIFICATION_TOKEN secret');
      }
      console.log(`  ✓ VERIFICATION_TOKEN`);

      if (!pushSecret('ARKE_VERIFY_AGENT_ID', klados.id)) {
        throw new Error('Failed to push ARKE_VERIFY_AGENT_ID secret');
      }
      console.log(`  ✓ ARKE_VERIFY_AGENT_ID`);

      // Step 5: Deploy worker
      console.log(`\n🚀 Deploying worker...`);
      if (!deployWorker()) {
        throw new Error('Failed to deploy worker');
      }

      // Step 6: Wait for deployment
      console.log(`\n⏳ Waiting for deployment to propagate...`);
      const isReady = await waitForDeployment(config.endpoint);
      if (!isReady) {
        console.warn('  ⚠️  Health check timed out, attempting verification anyway...');
      } else {
        console.log(`  ✓ Worker is responding`);
      }

      // Step 7: Confirm verification
      console.log(`\n🔍 Verifying endpoint ownership...`);
      const verifyResult = await confirmVerification(network, klados.id);
      if (!verifyResult.verified) {
        throw new Error(`Endpoint verification failed: ${verifyResult.error}`);
      }

      // Step 8: Activate klados
      console.log(`\n🎯 Activating klados...`);
      await activateKlados(network, klados.id);

      // Step 9: Create klados API key (authenticates as the klados, not the user)
      console.log(`\n🔑 Creating klados API key...`);
      const apiKey = await createKladosApiKey(network, klados.id, `${config.label} - ${network}`);

      // Step 10: Push API key to Cloudflare
      console.log(`\n📤 Pushing API key to Cloudflare...`);
      if (!pushSecret('ARKE_AGENT_KEY', apiKey.key)) {
        console.warn('  ⚠️  Could not push API key automatically');
        console.warn(`     Run: wrangler secret put ARKE_AGENT_KEY`);
      } else {
        console.log(`  ✓ ARKE_AGENT_KEY`);
      }

      // Step 11: Cleanup verification secrets
      console.log(`\n🧹 Cleaning up verification secrets...`);
      deleteSecret('VERIFICATION_TOKEN');
      deleteSecret('ARKE_VERIFY_AGENT_ID');
      console.log(`  ✓ Verification secrets removed`);

      // Step 12: Update wrangler.jsonc with AGENT_ID
      console.log(`\n📝 Updating wrangler.jsonc...`);
      if (updateWranglerConfig(klados.id)) {
        console.log(`  ✓ AGENT_ID set to ${klados.id}`);
      } else {
        console.warn(`  ⚠️  Could not update wrangler.jsonc`);
        console.warn(`     Set AGENT_ID manually: "${klados.id}"`);
      }

      // Step 13: Final deploy with API key and correct AGENT_ID
      console.log(`\n🚀 Final deployment...`);
      deployWorker();

      // Save state
      const state: AgentState = {
        klados_id: klados.id,
        collection_id: collectionId,
        api_key_prefix: getKeyPrefix(apiKey.key),
        endpoint: config.endpoint,
        endpoint_verified_at: verifyResult.verifiedAt,
        registered_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
      writeFileSync(stateFile, JSON.stringify(state, null, 2));

      // Done!
      console.log(`\n${'='.repeat(60)}`);
      console.log(`✅ Klados registered and activated!`);
      console.log(`${'='.repeat(60)}`);
      console.log(`   ID: ${klados.id}`);
      console.log(`   Collection: ${collectionId}`);
      console.log(`   Endpoint: ${config.endpoint}`);
      console.log(`   API Key: ${apiKey.key}`);
      console.log(`${'='.repeat(60)}\n`);
    }
  } catch (error) {
    console.error(`\n❌ Registration failed:`);
    console.error(`   ${error instanceof Error ? error.message : error}`);
    process.exit(1);
  }
}

main();
