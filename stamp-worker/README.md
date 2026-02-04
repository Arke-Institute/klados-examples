# Stamp Worker

A simple klados worker example that stamps entities with metadata. This is a great starting point for learning how to build klados workers.

## What It Does

When invoked, this worker:
1. Fetches the target entity
2. Appends a stamp entry to the entity's `stamps` array
3. Returns the stamped entity

**Stamps accumulate** - if the worker is invoked multiple times (e.g., in a workflow chain), each invocation adds a new stamp without overwriting previous ones.

Each stamp entry contains:
- `stamped_at` - ISO timestamp of when stamped
- `stamped_by` - The klados agent ID
- `stamp_number` - Sequential number (1, 2, 3...)
- `stamp_message` - "Stamp #N - This worker was here!"
- `job_id` - The job ID that created this stamp

The entity also gets:
- `stamps` - Array of all stamp entries
- `stamp_count` - Total number of stamps

## Quick Start

### 1. Install dependencies

```bash
npm install
```

### 2. Configure your endpoint

Edit `agent.json` and update the endpoint URL to match your Cloudflare subdomain:
```json
{
  "endpoint": "https://stamp-worker.YOUR-SUBDOMAIN.workers.dev"
}
```

Also update `wrangler.jsonc` if you want to change the worker name.

### 3. Register with Arke

```bash
ARKE_USER_KEY=uk_... npm run register
```

This will:
- Create your klados on Arke
- Deploy the worker to Cloudflare
- Configure the API key
- Save state to `.klados-state.json`

### 4. Run the tests

```bash
ARKE_USER_KEY=uk_... KLADOS_ID=<from-registration> npm test
```

The test creates an entity, invokes the stamp worker, and verifies the entity was stamped correctly.

## Project Structure

```
stamp-worker/
├── src/
│   ├── index.ts    # Hono router + request handling
│   ├── job.ts      # Stamp processing logic
│   └── types.ts    # Type definitions
├── test/
│   └── worker.test.ts  # E2E tests
├── scripts/
│   └── register.ts     # Registration script
├── agent.json          # Klados configuration
├── wrangler.jsonc      # Cloudflare config
└── vitest.config.ts    # Test configuration
```

## The Processing Logic

The core logic in `src/job.ts` accumulates stamps in an array:

```typescript
export async function processJob(job: KladosJob): Promise<string[]> {
  const target = await job.fetchTarget();

  // Get existing stamps or initialize empty array
  const existingStamps = Array.isArray(target.properties.stamps)
    ? target.properties.stamps
    : [];

  // Create new stamp entry
  const newStamp = {
    stamped_at: new Date().toISOString(),
    stamped_by: job.config.agentId,
    stamp_number: existingStamps.length + 1,
    stamp_message: `Stamp #${existingStamps.length + 1} - This worker was here!`,
    job_id: job.request.job_id,
  };

  // Accumulate stamps
  const stamps = [...existingStamps, newStamp];

  // Update entity with accumulated stamps
  await job.client.api.PUT('/entities/{id}', {
    params: { path: { id: target.id } },
    body: {
      expect_tip: tipData.cid,
      properties: { ...target.properties, stamps, stamp_count: stamps.length },
    },
  });

  return [target.id];
}
```

## Learn More

- [klados-worker-template](https://github.com/Arke-Institute/klados-worker-template) - Full template documentation
- [@arke-institute/rhiza](https://www.npmjs.com/package/@arke-institute/rhiza) - Workflow protocol library
- [@arke-institute/klados-testing](https://www.npmjs.com/package/@arke-institute/klados-testing) - Testing utilities

## License

MIT
