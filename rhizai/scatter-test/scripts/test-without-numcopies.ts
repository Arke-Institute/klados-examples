#!/usr/bin/env npx tsx
/**
 * Test script to verify scatter works without numCopies in log messages
 *
 * This creates a simple scatter workflow and verifies that the tree traversal
 * correctly determines expected children from the handoff outputs field.
 */

import {
  configureTestClient,
  createCollection,
  createEntity,
  createRhiza,
  invokeRhiza,
  waitForWorkflowTree,
} from '@arke-institute/klados-testing';

const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const SCATTER_KLADOS = process.env.SCATTER_KLADOS;
const STAMP_KLADOS = process.env.STAMP_KLADOS;

async function main() {
  if (!ARKE_USER_KEY || !SCATTER_KLADOS || !STAMP_KLADOS) {
    console.error('Missing required env vars: ARKE_USER_KEY, SCATTER_KLADOS, STAMP_KLADOS');
    process.exit(1);
  }

  console.log('Configuring test client...');
  configureTestClient({
    apiBase: 'https://arke-v1.arke.institute',
    userKey: ARKE_USER_KEY,
    network: 'test',
  });

  console.log('Creating test collection...');
  const collection = await createCollection({ label: 'Test Without NumCopies' });
  console.log(`  Collection: ${collection.id}`);

  console.log('Creating test entity with copy_count: 3...');
  const entity = await createEntity({
    type: 'test_entity',
    properties: {
      label: 'Test Source',
      copy_count: 3,
    },
    collectionId: collection.id,
  });
  console.log(`  Entity: ${entity.id}`);

  console.log('Creating nested scatter workflow...');
  const rhiza = await createRhiza({
    label: 'Test Nested Scatter (no numCopies)',
    description: 'Tests scatter → scatter → stamp without numCopies in log messages',
    version: '1.0',
    entry: 'scatter1',
    flow: {
      scatter1: {
        klados: { pi: SCATTER_KLADOS },
        then: { scatter: 'scatter2' },
      },
      scatter2: {
        klados: { pi: SCATTER_KLADOS },
        then: { scatter: 'stamp' },
      },
      stamp: {
        klados: { pi: STAMP_KLADOS },
        then: { done: true },
      },
    },
  });
  console.log(`  Rhiza: ${rhiza.id}`);

  console.log('\nInvoking workflow...');
  const result = await invokeRhiza({
    rhizaId: rhiza.id,
    targetEntity: entity.id,
    targetCollection: collection.id,
    confirm: true,
  });

  if (result.status !== 'started') {
    console.error('Workflow failed to start:', result);
    process.exit(1);
  }

  console.log(`  Job ID: ${result.job_id}`);
  console.log(`  Job Collection: ${result.job_collection}`);

  console.log('\nWaiting for workflow tree to complete...');
  console.log('Expected: 1 root + 3 level-2 scatters + 9 stamps = 13 logs\n');

  const tree = await waitForWorkflowTree(result.job_collection!, {
    timeout: 180000,
    pollInterval: 3000,
    onPoll: (t, elapsed) => {
      console.log(`[${Math.round(elapsed / 1000)}s] logs=${t.logs.size}, complete=${t.isComplete}, allChildrenDiscovered=${t.allChildrenDiscovered}`);
    },
  });

  console.log('\n=== RESULTS ===');
  console.log(`isComplete: ${tree.isComplete}`);
  console.log(`allChildrenDiscovered: ${tree.allChildrenDiscovered}`);
  console.log(`logs.size: ${tree.logs.size}`);
  console.log(`leaves.length: ${tree.leaves.length}`);
  console.log(`hasErrors: ${tree.hasErrors}`);

  if (tree.root) {
    console.log(`\nRoot expectedChildren: ${tree.root.expectedChildren}`);
    console.log(`Root actual children: ${tree.root.children.length}`);

    for (let i = 0; i < tree.root.children.length; i++) {
      const child = tree.root.children[i];
      console.log(`  Level-2[${i}] expectedChildren: ${child.expectedChildren}, actual: ${child.children.length}`);
    }
  }

  // Verify results
  const passed =
    tree.isComplete === true &&
    tree.allChildrenDiscovered === true &&
    tree.logs.size === 13 &&
    tree.hasErrors === false;

  if (passed) {
    console.log('\n✅ TEST PASSED - Framework correctly tracks outputs without numCopies!');
  } else {
    console.log('\n❌ TEST FAILED');
    console.log('Expected: isComplete=true, allChildrenDiscovered=true, logs.size=13, hasErrors=false');
    process.exit(1);
  }
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
