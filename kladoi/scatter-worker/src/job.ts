/**
 * Scatter Worker - Job Processing Logic
 *
 * This worker creates N copies of an entity for scatter testing.
 * Each copy is a new entity with a copy_index property.
 *
 * Used for E2E testing of scatter/gather workflows.
 */

import type { KladosJob } from '@arke-institute/rhiza';

/** Number of copies to create */
const NUM_COPIES = 3;

/**
 * Process a job by creating N copies of the target entity
 *
 * @param job - The KladosJob instance
 * @returns Array of output entity IDs (the created copies)
 */
export async function processJob(job: KladosJob): Promise<string[]> {
  job.log.info('Scatter worker starting', {
    target: job.request.target_entity,
    numCopies: NUM_COPIES,
    isWorkflow: job.isWorkflow,
  });

  // Fetch the target entity
  const target = await job.fetchTarget();
  job.log.info('Fetched target entity', {
    id: target.id,
    type: target.type,
    label: target.properties.label,
  });

  // Create N copies of the entity
  const copies: string[] = [];

  for (let i = 0; i < NUM_COPIES; i++) {
    const copyLabel = `${target.properties.label || 'Entity'} - Copy ${i + 1}`;

    // Create copy in the target collection (NOT job collection - that's only for logs)
    const { data, error } = await job.client.api.POST('/entities', {
      body: {
        type: target.type,
        collection: job.request.target_collection,
        properties: {
          ...target.properties,
          label: copyLabel,
          copy_index: i,
          copy_total: NUM_COPIES,
          source_entity: target.id,
          created_by: job.config.agentId,
          created_at: new Date().toISOString(),
        },
      },
    });

    if (error || !data) {
      throw new Error(`Failed to create copy ${i + 1}: ${JSON.stringify(error)}`);
    }

    copies.push(data.id);
    job.log.info(`Created copy ${i + 1}/${NUM_COPIES}`, {
      copyId: data.id,
      label: copyLabel,
    });
  }

  job.log.success('Scatter complete', {
    numCopies: copies.length,
    copyIds: copies,
  });

  // Return all copies - the rhiza flow's "scatter" handoff will invoke
  // the next step once per copy
  return copies;
}
