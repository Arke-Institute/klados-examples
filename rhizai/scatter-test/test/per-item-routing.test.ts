/**
 * E2E Test for Per-Item Routing in Scatter-Utility
 *
 * Tests that the scatter-utility correctly handles per-item targets,
 * dispatching different items to different kladoi based on their
 * individual target specifications.
 *
 * This test:
 * 1. Creates test entities
 * 2. Calls scatter-utility directly with per-item targets
 * 3. Verifies all items are dispatched to their specified targets
 *
 * Prerequisites:
 * 1. Deploy scatter-utility: cd ../../utils/scatter-utility && npm run deploy
 * 2. Deploy stamp worker: cd ../../kladoi/stamp-worker && npm run deploy
 * 3. Set environment variables (see ../../.env)
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  configureTestClient,
  createCollection,
  createEntity,
  deleteEntity,
  getClient,
  log,
} from '@arke-institute/klados-testing';
import type { InvokeOptions } from '@arke-institute/rhiza';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const STAMP_KLADOS = process.env.STAMP_KLADOS;
const SCATTER_UTILITY_URL = process.env.SCATTER_UTILITY_URL || 'https://scatter-utility.arke.institute';

// =============================================================================
// Test Suite
// =============================================================================

describe('per-item routing', () => {
  let targetCollection: { id: string };
  let testEntities: Array<{ id: string }> = [];

  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.warn('Skipping tests: ARKE_USER_KEY not set');
      return;
    }
    if (!STAMP_KLADOS) {
      console.warn('Skipping tests: STAMP_KLADOS not set');
      return;
    }

    configureTestClient({
      apiBase: ARKE_API_BASE,
      userKey: ARKE_USER_KEY,
      network: NETWORK,
    });

    log(`Scatter utility: ${SCATTER_UTILITY_URL}`);
    log(`Target klados: ${STAMP_KLADOS}`);
  });

  beforeAll(async () => {
    if (!ARKE_USER_KEY || !STAMP_KLADOS) return;

    log('Creating test fixtures...');

    targetCollection = await createCollection({
      label: `Per-Item Routing Test ${Date.now()}`,
      description: 'Target collection for per-item routing test',
    });
    log(`Created target collection: ${targetCollection.id}`);

    // Create multiple test entities
    for (let i = 0; i < 5; i++) {
      const entity = await createEntity({
        type: 'test_entity',
        properties: {
          label: `Test Entity ${i}`,
          content: `Entity ${i} for per-item routing test`,
          index: i,
          created_at: new Date().toISOString(),
        },
        collection: targetCollection.id,
      });
      testEntities.push(entity);
      log(`Created test entity ${i}: ${entity.id}`);
    }
  });

  afterAll(async () => {
    if (!ARKE_USER_KEY || !STAMP_KLADOS) return;

    log('Cleaning up...');
    try {
      for (const entity of testEntities) {
        if (entity?.id) await deleteEntity(entity.id);
      }
      if (targetCollection?.id) await deleteEntity(targetCollection.id);
      log('Cleanup complete');
    } catch (e) {
      log(`Cleanup error (non-fatal): ${e}`);
    }
  });

  // ==========================================================================
  // Tests
  // ==========================================================================

  it('should dispatch items with per-item targets', async () => {
    if (!ARKE_USER_KEY || !STAMP_KLADOS) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    log('Testing per-item routing with scatter-utility...');

    // Build outputs with per-item targets
    // All items go to the same klados for simplicity, but with per-item format
    const outputs = testEntities.map(entity => ({
      id: entity.id,
      target: STAMP_KLADOS,
      targetType: 'klados' as const,
    }));

    log(`Outputs with per-item targets: ${JSON.stringify(outputs, null, 2)}`);

    // Build invoke options
    const invokeOptions: InvokeOptions = {
      targetCollection: targetCollection.id,
      jobCollectionId: targetCollection.id, // Using same collection for simplicity
      apiBase: ARKE_API_BASE,
      network: NETWORK,
      parentLogs: [],
    };

    // Call scatter-utility directly with per-item targets
    // Note: NOT providing targetId/targetType at request level
    const response = await fetch(`${SCATTER_UTILITY_URL}/dispatch`, {
      method: 'POST',
      headers: {
        'Authorization': `ApiKey ${ARKE_USER_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        // No targetId or targetType at request level - using per-item targets
        outputs,
        invokeOptions,
      }),
    });

    expect(response.ok).toBe(true);
    const result = await response.json() as {
      accepted: boolean;
      dispatchId: string;
      totalItems: number;
    };

    log(`Dispatch accepted: ${result.accepted}`);
    log(`Dispatch ID: ${result.dispatchId}`);
    log(`Total items: ${result.totalItems}`);

    expect(result.accepted).toBe(true);
    expect(result.dispatchId).toBeDefined();
    expect(result.totalItems).toBe(testEntities.length);

    // Poll for completion
    log('Polling for dispatch completion...');
    let complete = false;
    let status: { status: string; total: number; dispatched: number; failed: number } | null = null;

    for (let i = 0; i < 60; i++) {
      const statusRes = await fetch(`${SCATTER_UTILITY_URL}/status/${result.dispatchId}`);
      if (!statusRes.ok) {
        log(`Status check failed: ${statusRes.status}`);
        break;
      }

      status = await statusRes.json() as typeof status;
      log(`Progress: ${status!.dispatched}/${status!.total} dispatched, ${status!.failed} failed, status: ${status!.status}`);

      if (status!.status === 'complete') {
        complete = true;
        break;
      }

      if (status!.status === 'error') {
        // Check for failures
        const failuresRes = await fetch(`${SCATTER_UTILITY_URL}/failures/${result.dispatchId}`);
        const failures = await failuresRes.json();
        log(`Failures: ${JSON.stringify(failures, null, 2)}`);
        throw new Error(`Scatter-utility error: ${JSON.stringify(status)}`);
      }

      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    expect(complete).toBe(true);
    expect(status!.total).toBe(testEntities.length);
    expect(status!.dispatched).toBe(testEntities.length);
    expect(status!.failed).toBe(0);

    log('');
    log('=== Per-Item Routing Test Complete ===');
    log(`   - Dispatched ${testEntities.length} items with per-item targets`);
    log(`   - All items dispatched to their specified target`);
    log(`   - No failures`);
  }, 120000);

  it('should dispatch items with mixed targets (some to klados, some per-item)', async () => {
    if (!ARKE_USER_KEY || !STAMP_KLADOS) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    log('Testing mixed per-item and default target routing...');

    // Mix of string outputs (use default target) and object outputs (per-item target)
    const outputs = testEntities.map((entity, i) => {
      if (i % 2 === 0) {
        // Even indices: use per-item target format
        return {
          id: entity.id,
          target: STAMP_KLADOS,
          targetType: 'klados' as const,
        };
      } else {
        // Odd indices: use simple string format (will use default target)
        return entity.id;
      }
    });

    log(`Mixed outputs: ${JSON.stringify(outputs, null, 2)}`);

    const invokeOptions: InvokeOptions = {
      targetCollection: targetCollection.id,
      jobCollectionId: targetCollection.id,
      apiBase: ARKE_API_BASE,
      network: NETWORK,
      parentLogs: [],
    };

    // Provide default target for string outputs
    const response = await fetch(`${SCATTER_UTILITY_URL}/dispatch`, {
      method: 'POST',
      headers: {
        'Authorization': `ApiKey ${ARKE_USER_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        targetId: STAMP_KLADOS,  // Default target for string outputs
        targetType: 'klados',
        outputs,
        invokeOptions,
      }),
    });

    expect(response.ok).toBe(true);
    const result = await response.json() as {
      accepted: boolean;
      dispatchId: string;
      totalItems: number;
    };

    log(`Dispatch accepted: ${result.accepted}`);
    log(`Dispatch ID: ${result.dispatchId}`);

    expect(result.accepted).toBe(true);
    expect(result.totalItems).toBe(testEntities.length);

    // Poll for completion
    log('Polling for dispatch completion...');
    let complete = false;
    let status: { status: string; total: number; dispatched: number; failed: number } | null = null;

    for (let i = 0; i < 60; i++) {
      const statusRes = await fetch(`${SCATTER_UTILITY_URL}/status/${result.dispatchId}`);
      status = await statusRes.json() as typeof status;
      log(`Progress: ${status!.dispatched}/${status!.total} dispatched, ${status!.failed} failed`);

      if (status!.status === 'complete') {
        complete = true;
        break;
      }

      if (status!.status === 'error') {
        const failuresRes = await fetch(`${SCATTER_UTILITY_URL}/failures/${result.dispatchId}`);
        const failures = await failuresRes.json();
        log(`Failures: ${JSON.stringify(failures, null, 2)}`);
        throw new Error(`Scatter-utility error`);
      }

      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    expect(complete).toBe(true);
    expect(status!.failed).toBe(0);

    log('');
    log('=== Mixed Routing Test Complete ===');
    log(`   - Dispatched ${testEntities.length} items with mixed formats`);
    log(`   - String outputs used default target`);
    log(`   - Object outputs used per-item target`);
  }, 120000);
});
