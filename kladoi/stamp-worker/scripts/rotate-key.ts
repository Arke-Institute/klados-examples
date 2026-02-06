#!/usr/bin/env npx tsx
/**
 * Rotate API Key Script
 *
 * Creates a new API key, pushes it to Cloudflare, and optionally revokes the old one.
 *
 * Usage:
 *   ARKE_USER_KEY=uk_... npx tsx scripts/rotate-key.ts              # Test network
 *   ARKE_USER_KEY=uk_... npx tsx scripts/rotate-key.ts --production # Main network
 *   ARKE_USER_KEY=uk_... npx tsx scripts/rotate-key.ts --revoke-old # Revoke old key
 */

import { execSync } from 'child_process';
import { ArkeClient } from '@arke-institute/sdk';
import {
  rotateApiKey,
  listApiKeys,
  readState,
  writeState,
  getStateFilePath,
  type KladosRegistrationState,
} from '@arke-institute/rhiza/registration';
import { CloudflareKeyStore } from '../../../../shared/cloudflare-keystore';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_USER_KEY = process.env.ARKE_USER_KEY;

// =============================================================================
// Main
// =============================================================================

async function main() {
  if (!ARKE_USER_KEY) {
    console.error('Error: ARKE_USER_KEY environment variable is required');
    process.exit(1);
  }

  const isProduction =
    process.argv.includes('--production') || process.argv.includes('--prod');
  const revokeOld = process.argv.includes('--revoke-old');
  const network = isProduction ? 'main' : 'test';

  console.log(`\n🔑 API Key Rotation (${network} network)\n`);

  // Load existing state
  const stateFile = getStateFilePath('.klados-state', network);
  const state = readState<KladosRegistrationState>(stateFile);

  if (!state) {
    console.error('Error: No klados registered. Run register first.');
    process.exit(1);
  }

  console.log(`Klados: ${state.klados_id}`);
  console.log(`Current key prefix: ${state.api_key_prefix}...`);
  console.log('');

  // Create client
  const client = new ArkeClient({ authToken: ARKE_USER_KEY, network });

  // Create key store
  const keyStore = new CloudflareKeyStore(process.cwd());

  try {
    // List current keys
    const keys = await listApiKeys(client, state.klados_id);
    console.log(`Found ${keys.length} existing key(s):`);
    for (const key of keys) {
      console.log(`  - ${key.prefix}... (${key.label})`);
    }
    console.log('');

    // Find current key by prefix
    const currentKey = keys.find((k) => k.prefix === state.api_key_prefix);

    // Rotate
    console.log('Creating new API key...');
    const result = await rotateApiKey(client, state.klados_id, {
      label: `Rotated ${new Date().toISOString()}`,
      revokeOld,
      oldKeyId: currentKey?.id,
      keyStore,
    });

    console.log(`  New key: ${result.new_key.prefix}...`);
    if (result.revoked_key_id) {
      console.log(`  Revoked: ${currentKey?.prefix}...`);
    }

    // Update state with new key prefix
    state.api_key_prefix = result.new_key.prefix;
    state.updated_at = new Date().toISOString();
    writeState(stateFile, state);

    // Redeploy to pick up new secret
    console.log('\n🚀 Redeploying worker...');
    execSync('wrangler deploy', { stdio: 'inherit' });

    console.log(`\n${'='.repeat(60)}`);
    console.log('✅ Key rotation complete!');
    console.log(`${'='.repeat(60)}`);
    console.log(`   New key prefix: ${result.new_key.prefix}...`);
    if (result.revoked_key_id) {
      console.log(`   Old key revoked: ${result.revoked_key_id}`);
    } else {
      console.log('   Old key still active (use --revoke-old to revoke)');
    }
    console.log(`${'='.repeat(60)}\n`);
  } catch (error) {
    console.error('\n❌ Key rotation failed:');
    console.error(`   ${error instanceof Error ? error.message : error}`);
    process.exit(1);
  }
}

main();
