/**
 * Direct test for Scatter Worker
 *
 * Tests the scatter worker in isolation (not via rhiza workflow).
 * Verifies that the worker:
 * 1. Accepts a single entity
 * 2. Creates N copies
 * 3. Returns copy IDs as outputs
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  configureTestClient,
  createCollection,
  createEntity,
  getEntity,
  deleteEntity,
  invokeKlados,
  waitForKladosLog,
  log,
  apiRequest,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const SCATTER_KLADOS = process.env.SCATTER_KLADOS;

// Number of copies the scatter worker creates
const NUM_COPIES = 3;

// =============================================================================
// Test Suite
// =============================================================================

describe('scatter worker (direct)', () => {
  let targetCollection: { id: string };
  let testEntity: { id: string };
  let jobCollectionId: string; // Returned from API

  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.warn('Skipping tests: ARKE_USER_KEY not set');
      return;
    }
    if (!SCATTER_KLADOS) {
      console.warn('Skipping tests: SCATTER_KLADOS not set');
      return;
    }

    configureTestClient({
      apiBase: ARKE_API_BASE,
      userKey: ARKE_USER_KEY,
      network: NETWORK,
    });

    log(`Using scatter klados: ${SCATTER_KLADOS}`);
  });

  beforeAll(async () => {
    if (!ARKE_USER_KEY || !SCATTER_KLADOS) return;

    log('Creating test fixtures...');

    targetCollection = await createCollection({
      label: `Scatter Test Targets ${Date.now()}`,
      description: 'Target collection for scatter worker test',
    });
    log(`Created target collection: ${targetCollection.id}`);

    testEntity = await createEntity({
      type: 'test_entity',
      properties: {
        label: 'Test Entity for Scatter',
        content: 'This entity will be copied 3 times',
        created_at: new Date().toISOString(),
      },
      collection: targetCollection.id,
    });
    log(`Created test entity: ${testEntity.id}`);
  });

  afterAll(async () => {
    if (!ARKE_USER_KEY || !SCATTER_KLADOS) return;

    log('Cleaning up test fixtures...');
    try {
      if (testEntity?.id) await deleteEntity(testEntity.id);
      if (targetCollection?.id) await deleteEntity(targetCollection.id);
      if (jobCollectionId) await deleteEntity(jobCollectionId);
      log('Cleanup complete');
    } catch (e) {
      log(`Cleanup error (non-fatal): ${e}`);
    }
  });

  // ==========================================================================
  // Tests
  // ==========================================================================

  it('should create N copies when invoked directly', async () => {
    if (!ARKE_USER_KEY || !SCATTER_KLADOS) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Invoke the scatter klados directly (no rhiza context)
    // Don't pass jobCollection - let the API create one
    log('Invoking scatter klados directly...');
    const result = await invokeKlados({
      kladosId: SCATTER_KLADOS,
      targetEntity: testEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    expect(result.status).toBe('started');
    expect(result.job_id).toBeDefined();
    expect(result.job_collection).toBeDefined();

    jobCollectionId = result.job_collection!;
    log(`Klados invoked: ${result.job_id}`);
    log(`Job collection: ${jobCollectionId}`);

    // Wait for the klados to complete
    log('Waiting for klados to complete...');
    const logEntry = await waitForKladosLog(jobCollectionId, {
      timeout: 60000,
      pollInterval: 2000,
    });

    expect(logEntry).not.toBeNull();
    log(`Log status: ${logEntry!.properties.status}`);
    expect(logEntry!.properties.status).toBe('done');

    // Get the copy IDs from the log data
    const logData = logEntry!.properties.log_data;
    const successLog = logData.messages.find(
      (m: { message: string }) => m.message === 'Scatter complete'
    );
    expect(successLog).toBeDefined();
    const copyIds = successLog?.metadata?.copyIds as string[];
    log(`Created ${copyIds.length} copies`);
    expect(copyIds.length).toBe(NUM_COPIES);

    // Verify each copy exists and has the expected properties
    for (const copyId of copyIds) {
      const entity = await getEntity(copyId);
      log(`Copy ${entity.properties.copy_index}: ${entity.properties.label}`);

      expect(entity.properties.source_entity).toBe(testEntity.id);
      expect(entity.properties.copy_total).toBe(NUM_COPIES);
      expect(entity.properties.copy_index).toBeGreaterThanOrEqual(0);
      expect(entity.properties.copy_index).toBeLessThan(NUM_COPIES);
    }

    log('✅ Scatter worker created copies successfully!');
  });
});
