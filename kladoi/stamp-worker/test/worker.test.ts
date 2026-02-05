/**
 * E2E Test for Stamp Worker
 *
 * This test invokes the stamp worker against the Arke API and verifies:
 * 1. The worker accepts and processes jobs correctly
 * 2. The target entity is stamped with metadata
 * 3. Log entries are properly recorded
 *
 * Prerequisites:
 * 1. Deploy your worker: npm run deploy
 * 2. Register the klados: npm run register
 * 3. Set environment variables (see below)
 *
 * Environment variables:
 *   ARKE_USER_KEY   - Your Arke user API key (uk_...)
 *   KLADOS_ID       - The klados entity ID from registration
 *   ARKE_API_BASE   - API base URL (default: https://arke-v1.arke.institute)
 *   ARKE_NETWORK    - Network to use (default: test)
 *
 * Usage:
 *   ARKE_USER_KEY=uk_... KLADOS_ID=klados_... npm test
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
  assertLogCompleted,
  assertLogHasMessages,
  sleep,
  log,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const KLADOS_ID = process.env.KLADOS_ID;

// =============================================================================
// Test Suite
// =============================================================================

describe('stamp-worker', () => {
  // Test fixtures
  let targetCollection: { id: string };
  let testEntity: { id: string };
  let jobCollectionId: string; // Returned by API (not created by us)

  // Skip tests if environment not configured
  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.warn('Skipping tests: ARKE_USER_KEY not set');
      return;
    }
    if (!KLADOS_ID) {
      console.warn('Skipping tests: KLADOS_ID not set');
      return;
    }

    // Configure the test client
    configureTestClient({
      apiBase: ARKE_API_BASE,
      userKey: ARKE_USER_KEY,
      network: NETWORK,
    });
  });

  // Create test fixtures
  beforeAll(async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) return;

    log('Creating test fixtures...');

    // Create target collection - this is where your entities live and work happens
    // Note: We do NOT create a job collection - the API creates one automatically
    // and returns it in the invoke response. Job collections are ONLY for logs.
    targetCollection = await createCollection({
      label: `Stamp Target ${Date.now()}`,
      description: 'Target collection for stamp worker test',
    });
    log(`Created target collection: ${targetCollection.id}`);

    // Create test entity
    testEntity = await createEntity({
      type: 'test_entity',
      properties: {
        title: 'Test Entity for Stamping',
        content: 'This entity will be stamped',
        created_at: new Date().toISOString(),
      },
      collectionId: targetCollection.id,
    });
    log(`Created test entity: ${testEntity.id}`);
  });

  // Cleanup test fixtures
  afterAll(async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) return;

    log('Cleaning up test fixtures...');

    try {
      if (testEntity?.id) await deleteEntity(testEntity.id);
      if (targetCollection?.id) await deleteEntity(targetCollection.id);
      // Note: We don't clean up jobCollectionId - it's owned by the API
      log('Cleanup complete');
    } catch (e) {
      log(`Cleanup error (non-fatal): ${e}`);
    }
  });

  // ==========================================================================
  // Tests
  // ==========================================================================

  it('should stamp entity with metadata', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Invoke the klados
    // Note: We don't pass jobCollection - the API creates one and returns it
    log('Invoking stamp klados...');
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: testEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    expect(result.status).toBe('started');
    expect(result.job_id).toBeDefined();
    expect(result.job_collection).toBeDefined();

    jobCollectionId = result.job_collection!;
    log(`Job started: ${result.job_id}`);
    log(`Job collection: ${jobCollectionId}`);

    // Wait for stamp to be applied
    log('Waiting for entity to be stamped...');
    let stamped = false;
    const startTime = Date.now();
    const timeout = 30000;

    while (!stamped && Date.now() - startTime < timeout) {
      await sleep(2000);
      const entity = await getEntity(testEntity.id);

      // Check for array-based stamps
      if (Array.isArray(entity.properties.stamps) && entity.properties.stamps.length > 0) {
        stamped = true;
        const stamp = entity.properties.stamps[0];
        log('Entity stamped!');
        log(`  stamp_count: ${entity.properties.stamp_count}`);
        log(`  stamps[0].stamped_at: ${stamp.stamped_at}`);
        log(`  stamps[0].stamped_by: ${stamp.stamped_by}`);
        log(`  stamps[0].stamp_number: ${stamp.stamp_number}`);
        log(`  stamps[0].stamp_message: ${stamp.stamp_message}`);
      }
    }

    expect(stamped).toBe(true);

    // Verify stamp structure
    const finalEntity = await getEntity(testEntity.id);
    expect(finalEntity.properties.stamps).toHaveLength(1);
    expect(finalEntity.properties.stamp_count).toBe(1);
    expect(finalEntity.properties.stamps[0]).toMatchObject({
      stamp_number: 1,
      stamped_by: expect.any(String),
      stamped_at: expect.any(String),
      stamp_message: expect.stringContaining('Stamp #1'),
      job_id: result.job_id,
    });

    // Verify log entry
    log('Verifying klados log...');
    const kladosLog = await waitForKladosLog(jobCollectionId, {
      timeout: 30000,
      pollInterval: 2000,
    });

    assertLogCompleted(kladosLog);
    log(`Log status: ${kladosLog.properties.status}`);

    assertLogHasMessages(kladosLog, [
      { textContains: 'starting' },
      { textContains: 'Fetched target' },
      { textContains: 'stamped successfully' },
      { textContains: 'completed' },
    ]);
    log('Log messages verified');

    // Show all messages
    for (const msg of kladosLog.properties.log_data.messages) {
      log(`  [${msg.level}] ${msg.message}`);
    }
  });

  it('should handle preview mode (confirm=false)', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    const preview = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: testEntity.id,
      targetCollection: targetCollection.id,
      confirm: false,
    });

    expect(preview.status).toBe('pending_confirmation');
    log(`Preview result: ${preview.status}`);
  });
});
