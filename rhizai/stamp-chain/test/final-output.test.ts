/**
 * Test for final_output and final_error relationships on job collection
 *
 * Verifies that terminal workflow nodes add relationships to the job collection:
 * - final_output: when a workflow completes successfully with `done`
 * - final_error: when a workflow fails
 *
 * This enables O(1) discovery of workflow outputs without tree traversal.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  configureTestClient,
  createCollection,
  createEntity,
  deleteEntity,
  invokeRhiza,
  waitForWorkflowTree,
  getEntity,
  log,
} from '@arke-institute/klados-testing';

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';

// From .rhiza-state.json
const RHIZA_ID = 'IIKGNVQ7595G35N0B1R2HF5PJK';

describe('final_output relationship', () => {
  let targetCollection: { id: string };
  let testEntity: { id: string };

  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.warn('Skipping tests: ARKE_USER_KEY not set');
      return;
    }

    configureTestClient({
      apiBase: ARKE_API_BASE,
      userKey: ARKE_USER_KEY,
      network: NETWORK,
    });
  });

  beforeAll(async () => {
    if (!ARKE_USER_KEY) return;

    log('Creating test fixtures...');

    targetCollection = await createCollection({
      label: `Final Output Test ${Date.now()}`,
      description: 'Test collection for final_output feature',
    });
    log(`Created target collection: ${targetCollection.id}`);

    testEntity = await createEntity({
      type: 'test_entity',
      properties: {
        title: 'Test Entity for Final Output',
        content: 'This entity will be processed by stamp-chain',
      },
      collection: targetCollection.id,
    });
    log(`Created test entity: ${testEntity.id}`);
  });

  afterAll(async () => {
    if (!ARKE_USER_KEY) return;

    log('Cleaning up...');
    try {
      if (testEntity?.id) await deleteEntity(testEntity.id);
      if (targetCollection?.id) await deleteEntity(targetCollection.id);
    } catch (e) {
      log(`Cleanup error: ${e}`);
    }
  });

  it('should add terminal relationship (final_output or final_error) to job collection', async () => {
    if (!ARKE_USER_KEY) {
      console.warn('Test skipped: ARKE_USER_KEY not set');
      return;
    }

    // Invoke the stamp-chain rhiza workflow
    log('Invoking stamp-chain rhiza...');
    const result = await invokeRhiza({
      rhizaId: RHIZA_ID,
      targetEntity: testEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    expect(result.status).toBe('started');
    expect(result.job_collection).toBeDefined();

    const jobCollectionId = result.job_collection!;
    log(`Job collection: ${jobCollectionId}`);

    // Wait for workflow to complete using tree traversal
    log('Waiting for workflow to complete...');
    const tree = await waitForWorkflowTree(jobCollectionId, {
      timeout: 120000,
      pollInterval: 3000,
      onPoll: (t, elapsed) => {
        log(`  ${t.logs.size} logs, complete=${t.isComplete}, elapsed=${Math.round(elapsed / 1000)}s`);
      },
    });

    expect(tree.isComplete).toBe(true);
    log(`Workflow complete with ${tree.logs.size} logs`);

    // Check job collection relationships
    log('Checking job collection relationships...');
    const jobCollection = await getEntity(jobCollectionId);

    // API returns 'relationships' not 'relationships_out'
    const relationships = (jobCollection as any).relationships || [];

    // Check for first_log (entry point)
    const firstLogRels = relationships.filter(
      (r: { predicate: string }) => r.predicate === 'first_log'
    );
    log(`Found ${firstLogRels.length} first_log relationship(s)`);
    expect(firstLogRels.length).toBe(1);

    const finalOutputRels = relationships.filter(
      (r: { predicate: string }) => r.predicate === 'final_output'
    );
    const finalErrorRels = relationships.filter(
      (r: { predicate: string }) => r.predicate === 'final_error'
    );

    log(`Found ${finalOutputRels.length} final_output relationship(s)`);
    log(`Found ${finalErrorRels.length} final_error relationship(s)`);

    // Should have at least one terminal relationship (either success or error)
    const terminalRels = [...finalOutputRels, ...finalErrorRels];
    expect(terminalRels.length).toBeGreaterThan(0);

    for (const rel of terminalRels) {
      log(`  ${rel.predicate} -> ${rel.peer}`);

      // Verify the terminal log has the correct status
      const terminalLog = await getEntity(rel.peer);
      const expectedStatus = rel.predicate === 'final_output' ? 'done' : 'error';
      expect(terminalLog.properties.status).toBe(expectedStatus);
      log(`  Log ${rel.peer} has status: ${terminalLog.properties.status}`);
    }
  });
});
