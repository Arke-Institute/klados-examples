#!/usr/bin/env npx tsx
/**
 * Rhiza Workflow Registration Script
 *
 * Automated registration flow using @arke-institute/rhiza registration module:
 * - Creates new rhiza workflows
 * - Updates existing rhiza workflows if version/flow changes
 * - Substitutes environment variables in workflow definitions
 * - Supports dry-run mode to preview changes
 *
 * Usage:
 *   ARKE_USER_KEY=uk_... npx tsx scripts/register.ts stamp-chain        # Test network
 *   ARKE_USER_KEY=uk_... npx tsx scripts/register.ts stamp-chain --prod # Main network
 *   ARKE_USER_KEY=uk_... npx tsx scripts/register.ts stamp-chain --dry-run
 */

import * as fs from 'fs';
import * as path from 'path';
import { ArkeClient } from '@arke-institute/sdk';
import {
  syncRhiza,
  readState,
  writeState,
  getStateFilePath,
  type RhizaConfig,
  type RhizaRegistrationState,
  type DryRunResult,
  type SyncResult,
} from '@arke-institute/rhiza/registration';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_USER_KEY = process.env.ARKE_USER_KEY;

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Recursively substitute environment variables in workflow definitions.
 * Values starting with $ are replaced with the corresponding env var.
 */
function substituteEnvVars(obj: unknown): unknown {
  if (typeof obj === 'string') {
    if (obj.startsWith('$')) {
      const envVar = obj.slice(1);
      const value = process.env[envVar];
      if (!value) {
        throw new Error(`Environment variable ${envVar} is not set`);
      }
      return value;
    }
    return obj;
  }

  if (Array.isArray(obj)) {
    return obj.map(substituteEnvVars);
  }

  if (obj && typeof obj === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
      // Substitute in both keys and values
      const newKey =
        typeof key === 'string' && key.startsWith('$')
          ? (process.env[key.slice(1)] ?? key)
          : key;
      result[newKey] = substituteEnvVars(value);
    }
    return result;
  }

  return obj;
}

function isDryRunResult(
  result: SyncResult<RhizaRegistrationState> | DryRunResult
): result is DryRunResult {
  return (
    result.action === 'would_create' ||
    result.action === 'would_update' ||
    (result.action === 'unchanged' && !('state' in result))
  );
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  if (!ARKE_USER_KEY) {
    console.error('Error: ARKE_USER_KEY environment variable is required');
    process.exit(1);
  }

  // Get workflow name from args
  const workflowArg = process.argv[2];
  if (!workflowArg || workflowArg.startsWith('--')) {
    console.error('Usage: npm run register -- <workflow-name> [--production] [--dry-run]');
    console.error('Example: npm run register -- stamp-chain');
    process.exit(1);
  }

  const isProduction =
    process.argv.includes('--production') || process.argv.includes('--prod');
  const isDryRun = process.argv.includes('--dry-run');
  const network = isProduction ? 'main' : 'test';

  // Resolve workflow file
  const workflowName = workflowArg.replace(/\.json$/, '');
  const workflowFile = path.join('workflows', `${workflowName}.json`);

  if (!fs.existsSync(workflowFile)) {
    console.error(`Error: Workflow file not found: ${workflowFile}`);
    process.exit(1);
  }

  console.log(`\n📦 Rhiza Registration (${network} network)${isDryRun ? ' [DRY RUN]' : ''}\n`);
  console.log(`Workflow: ${workflowName}`);

  // Load and parse workflow definition
  const rawContent = fs.readFileSync(workflowFile, 'utf-8');
  const rawWorkflow = JSON.parse(rawContent);

  // Substitute environment variables
  let config: RhizaConfig;
  try {
    config = substituteEnvVars(rawWorkflow) as RhizaConfig;
  } catch (error) {
    console.error(`\nError: ${(error as Error).message}`);
    console.error('Make sure all required environment variables are set.');
    console.error('See .env.example for required variables.');
    process.exit(1);
  }

  console.log(`Label: ${config.label}`);
  console.log(`Version: ${config.version}`);
  console.log(`Entry: ${config.entry}`);
  console.log(`Steps: ${Object.keys(config.flow).length}`);
  for (const [stepName, step] of Object.entries(config.flow)) {
    console.log(`  - ${stepName}: ${step.klados.pi}`);
  }
  console.log('');

  // Load existing state (per-workflow state file)
  const stateFile = getStateFilePath(`.rhiza-state-${workflowName}`, network);
  const state = readState<RhizaRegistrationState>(stateFile);

  if (state) {
    console.log(`Found existing rhiza: ${state.rhiza_id}`);
  } else {
    console.log('Creating new rhiza...\n');
  }

  // Create client
  const client = new ArkeClient({ authToken: ARKE_USER_KEY, network });

  try {
    // Sync rhiza
    const result = await syncRhiza(client, config, state, {
      network,
      dryRun: isDryRun,
      collectionLabel: `Rhiza: ${config.label}`,
    });

    // Handle dry run result
    if (isDryRunResult(result)) {
      console.log(`\n📋 Would: ${result.action}`);
      if (result.changes && result.changes.length > 0) {
        console.log('\nChanges:');
        for (const change of result.changes) {
          console.log(`  ${change.field}: ${change.from ?? '(none)'} → ${change.to}`);
        }
      }
      console.log('\nRun without --dry-run to apply changes.');
      return;
    }

    // Handle actual sync result
    const { action, state: newState } = result;

    // Save state
    if (action !== 'unchanged') {
      writeState(stateFile, newState);
    }

    // Print result
    console.log(`\n${'='.repeat(60)}`);
    console.log(`✅ Rhiza ${action}!`);
    console.log(`${'='.repeat(60)}`);
    console.log(`   ID: ${newState.rhiza_id}`);
    console.log(`   Collection: ${newState.collection_id}`);
    console.log(`   Version: ${newState.version}`);
    console.log(`${'='.repeat(60)}\n`);

    if (action === 'created') {
      console.log(`To run tests, set RHIZA_ID=${newState.rhiza_id} and run: npm test`);
    }
  } catch (error) {
    console.error('\n❌ Registration failed:');
    console.error(`   ${error instanceof Error ? error.message : error}`);
    process.exit(1);
  }
}

main();
