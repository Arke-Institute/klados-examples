/**
 * Register a rhiza workflow on the Arke network
 *
 * This script:
 * 1. Loads a workflow definition from workflows/*.json
 * 2. Substitutes environment variables for klados references
 * 3. Creates the rhiza entity on Arke
 * 4. Saves the rhiza ID to .rhiza-state.json
 *
 * Usage:
 *   ARKE_USER_KEY=uk_... npm run register -- stamp-chain
 *   ARKE_USER_KEY=uk_... npm run register -- stamp-chain.json
 */

import * as fs from 'fs';
import * as path from 'path';
import { configureTestClient, createRhiza, createCollection, apiRequest } from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const ARKE_NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';

const STATE_FILE = '.rhiza-state.json';

// =============================================================================
// Types
// =============================================================================

interface FlowStep {
  klados: { pi: string; type?: string };
  then:
    | { done: true }
    | { pass: string; route?: unknown[] }
    | { scatter: string; route?: unknown[] }
    | { gather: string; route?: unknown[] };
}

interface Workflow {
  label: string;
  description?: string;
  version: string;
  entry: string;
  flow: Record<string, FlowStep>;
}

// =============================================================================
// Helper Functions
// =============================================================================

function loadState(): Record<string, unknown> {
  try {
    const content = fs.readFileSync(STATE_FILE, 'utf-8');
    return JSON.parse(content);
  } catch {
    return {};
  }
}

function saveState(state: Record<string, unknown>): void {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

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
      const newKey = typeof key === 'string' && key.startsWith('$')
        ? (process.env[key.slice(1)] ?? key)
        : key;
      result[newKey] = substituteEnvVars(value);
    }
    return result;
  }

  return obj;
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  // Validate environment
  if (!ARKE_USER_KEY) {
    console.error('Error: ARKE_USER_KEY environment variable is required');
    process.exit(1);
  }

  // Get workflow name from args
  const workflowArg = process.argv[2];
  if (!workflowArg) {
    console.error('Usage: npm run register -- <workflow-name>');
    console.error('Example: npm run register -- stamp-chain');
    process.exit(1);
  }

  // Resolve workflow file
  const workflowName = workflowArg.replace(/\.json$/, '');
  const workflowFile = path.join('workflows', `${workflowName}.json`);

  if (!fs.existsSync(workflowFile)) {
    console.error(`Error: Workflow file not found: ${workflowFile}`);
    process.exit(1);
  }

  console.log(`\nRegistering workflow: ${workflowName}`);
  console.log(`Network: ${ARKE_NETWORK}`);
  console.log(`API: ${ARKE_API_BASE}\n`);

  // Load and parse workflow definition
  const rawContent = fs.readFileSync(workflowFile, 'utf-8');
  const rawWorkflow = JSON.parse(rawContent);

  // Substitute environment variables
  let workflow: Workflow;

  try {
    workflow = substituteEnvVars(rawWorkflow) as Workflow;
  } catch (error) {
    console.error(`Error: ${(error as Error).message}`);
    console.error('\nMake sure all required environment variables are set.');
    console.error('See .env.example for required variables.');
    process.exit(1);
  }

  console.log('Workflow configuration:');
  console.log(`  Label: ${workflow.label}`);
  console.log(`  Version: ${workflow.version}`);
  console.log(`  Entry step: ${workflow.entry}`);
  console.log(`  Flow steps: ${Object.keys(workflow.flow).length}`);
  for (const [stepName, step] of Object.entries(workflow.flow)) {
    console.log(`    - ${stepName}: klados=${step.klados.pi}`);
  }
  console.log('');

  // Configure test client
  configureTestClient({
    apiBase: ARKE_API_BASE,
    userKey: ARKE_USER_KEY,
    network: ARKE_NETWORK,
  });

  // Load existing state
  const state = loadState();

  // Create or reuse collection for the rhiza
  let collectionId = state.collectionId as string | undefined;

  if (!collectionId) {
    console.log('Creating collection for rhiza...');
    const collection = await createCollection({
      label: `Rhiza: ${workflow.label}`,
      description: `Collection for ${workflow.label} workflow`,
    });
    collectionId = collection.id;
    state.collectionId = collectionId;
    console.log(`  Collection ID: ${collectionId}`);
  } else {
    console.log(`Using existing collection: ${collectionId}`);
  }

  // Check if rhiza already exists
  let rhizaId = state.rhizaId as string | undefined;

  if (rhizaId) {
    console.log(`\nRhiza already registered: ${rhizaId}`);
    console.log('To re-register, delete .rhiza-state.json and run again.');
  } else {
    // Create the rhiza
    console.log('\nCreating rhiza entity...');
    const rhiza = await createRhiza({
      label: workflow.label,
      description: workflow.description,
      version: workflow.version,
      entry: workflow.entry,
      flow: workflow.flow,
      collectionId,
    });

    rhizaId = rhiza.id;
    state.rhizaId = rhizaId;
    console.log(`  Rhiza ID: ${rhizaId}`);
  }

  // Save state
  saveState(state);

  console.log('\n✓ Registration complete!');
  console.log(`\nTo run tests, set RHIZA_ID=${rhizaId} and run:`);
  console.log('  npm test');
}

main().catch((error) => {
  console.error('Registration failed:', error);
  process.exit(1);
});
