/**
 * Stamp Worker - Job Processing Logic
 *
 * This worker stamps entities with metadata to prove it processed them.
 * Used for E2E testing of the klados worker template.
 */

import type { KladosJob } from '@arke-institute/rhiza';

/**
 * Process a job by stamping the target entity
 *
 * @param job - The KladosJob instance
 * @returns Array of output entity IDs (the stamped entity)
 */
export async function processJob(job: KladosJob): Promise<string[]> {
  job.log.info('Stamp worker starting', {
    target: job.request.target_entity,
    isWorkflow: job.isWorkflow,
  });

  // Fetch the target entity
  const target = await job.fetchTarget();
  job.log.info('Fetched target entity', {
    id: target.id,
    type: target.type,
  });

  // Get current tip for CAS update
  const { data: tipData, error: tipError } = await job.client.api.GET('/entities/{id}/tip', {
    params: { path: { id: target.id } },
  });

  if (tipError || !tipData) {
    throw new Error(`Failed to get entity tip: ${target.id}`);
  }

  // Stamp the entity with metadata
  const stampProperties = {
    stamped_at: new Date().toISOString(),
    stamped_by: job.config.agentId,
    stamp_message: 'This worker was here!',
  };

  const { error: updateError } = await job.client.api.PUT('/entities/{id}', {
    params: { path: { id: target.id } },
    body: {
      expect_tip: tipData.cid,
      properties: {
        ...target.properties,
        ...stampProperties,
      },
    },
  });

  if (updateError) {
    throw new Error(`Failed to stamp entity: ${JSON.stringify(updateError)}`);
  }

  job.log.info('Entity stamped successfully', stampProperties);
  job.log.success('Job completed');

  // Return the stamped entity as output
  return [target.id];
}
