/**
 * Scatter Worker - Job Processing Logic
 *
 * This worker creates N copies of an entity for scatter testing.
 * Each copy is a new entity with a copy_index property.
 *
 * Used for E2E testing of scatter/gather workflows.
 */

import type { KladosJob } from '@arke-institute/rhiza';

/** Default number of copies to create */
const DEFAULT_COPY_COUNT = 3;

/** Batch size for concurrent entity creation */
const BATCH_SIZE = 20;

/**
 * Process a job by creating N copies of the target entity
 *
 * The number of copies is configurable via the target entity's `copy_count` property.
 * Defaults to 3 if not specified.
 *
 * @param job - The KladosJob instance
 * @returns Array of output entity IDs (the created copies)
 */
export async function processJob(job: KladosJob): Promise<string[]> {
  // Fetch the target entity first to get copy_count
  const target = await job.fetchTarget();

  // Read copy_count from target properties, default to 3
  const numCopies = (target.properties.copy_count as number) || DEFAULT_COPY_COUNT;

  job.log.info('Scatter worker starting', {
    target: job.request.target_entity,
    numCopies,
    isWorkflow: job.isWorkflow,
  });

  job.log.info('Fetched target entity', {
    id: target.id,
    type: target.type,
    label: target.properties.label,
    requestedCopies: target.properties.copy_count,
  });

  // Create N copies of the entity in parallel batches
  const copies: string[] = [];

  for (let batchStart = 0; batchStart < numCopies; batchStart += BATCH_SIZE) {
    const batchEnd = Math.min(batchStart + BATCH_SIZE, numCopies);
    const batchPromises: Promise<string>[] = [];

    for (let i = batchStart; i < batchEnd; i++) {
      const copyLabel = `${target.properties.label || 'Entity'} - Copy ${i + 1}`;

      // Create copy in the target collection with relationship back to original
      const promise = job.client.api.POST('/entities', {
        body: {
          type: target.type,
          collection: job.request.target_collection,
          properties: {
            ...target.properties,
            label: copyLabel,
            copy_index: i,
            copy_total: numCopies,
            source_entity: target.id,
            created_by: job.config.agentId,
            created_at: new Date().toISOString(),
          },
          // Relationship from copy to original
          relationships: [
            {
              predicate: 'copy_of',
              peer: target.id,
              peer_type: target.type,
              peer_label: target.properties.label as string,
            },
          ],
        },
      }).then(({ data, error }) => {
        if (error || !data) {
          throw new Error(`Failed to create copy ${i + 1}: ${JSON.stringify(error)}`);
        }
        return data.id;
      });

      batchPromises.push(promise);
    }

    // Wait for this batch to complete
    const batchResults = await Promise.all(batchPromises);
    copies.push(...batchResults);

    job.log.info(`Created batch ${Math.floor(batchStart / BATCH_SIZE) + 1}`, {
      batchStart,
      batchEnd,
      totalCreated: copies.length,
    });
  }

  // Update original entity to point to all copies
  // First get the current tip for CAS
  const { data: tipData, error: tipError } = await job.client.api.GET('/entities/{id}/tip', {
    params: { path: { id: target.id } },
  });

  if (tipError || !tipData) {
    job.log.info('Failed to get tip for original entity', { error: tipError });
  } else {
    const { error: updateError } = await job.client.api.PUT('/entities/{id}', {
      params: { path: { id: target.id } },
      body: {
        expect_tip: tipData.cid,
        relationships_add: copies.map((copyId, i) => ({
          predicate: 'has_copy',
          peer: copyId,
          peer_type: target.type,
          peer_label: `${target.properties.label || 'Entity'} - Copy ${i + 1}`,
        })),
      },
    });

    if (updateError) {
      job.log.info('Failed to add has_copy relationships to original', { error: updateError });
      // Don't fail the job - copies were created successfully
    }
  }

  job.log.success('Scatter complete', {
    numCopies: copies.length,
    copyIds: copies,
  });

  // Return all copies - the rhiza flow's "scatter" handoff will invoke
  // the next step once per copy
  return copies;
}
