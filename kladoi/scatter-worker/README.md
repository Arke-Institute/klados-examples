# Scatter Worker

A klados worker that creates N copies of an entity, designed for testing scatter/gather workflows.

## What It Does

When invoked, this worker:
1. Fetches the target entity
2. Creates N copies of it (default: 3)
3. Returns all copy IDs as outputs

When used in a rhiza workflow with `{ scatter: "next_step" }`, the SDK will invoke the next step once per copy, enabling parallel processing.

Each copy contains:
- Original properties from the source entity
- `copy_index` - 0-based index of this copy
- `copy_total` - Total number of copies created
- `source_entity` - ID of the original entity
- `created_by` - The klados agent ID
- `created_at` - ISO timestamp

## Quick Start

### 1. Install dependencies

```bash
npm install
```

### 2. Configure your endpoint

Edit `agent.json` and update the endpoint URL to match your Cloudflare subdomain:
```json
{
  "endpoint": "https://scatter-worker.YOUR-SUBDOMAIN.workers.dev"
}
```

### 3. Register with Arke

```bash
ARKE_USER_KEY=uk_... npm run register
```

This will:
- Create your klados on Arke
- Deploy the worker to Cloudflare
- Configure the API key
- Save state to `.klados-state.json`

## Project Structure

```
scatter-worker/
├── src/
│   ├── index.ts    # Hono router + request handling
│   ├── job.ts      # Copy creation logic
│   └── types.ts    # Type definitions
├── scripts/
│   └── register.ts # Registration script
├── agent.json      # Klados configuration
└── wrangler.jsonc  # Cloudflare config
```

## Usage in Workflows

This worker is designed to be used with scatter handoffs:

```json
{
  "entry": "scatter",
  "flow": {
    "scatter": {
      "klados": { "pi": "scatter_worker_id" },
      "then": { "scatter": "process" }
    },
    "process": {
      "klados": { "pi": "some_processor_id" },
      "then": { "done": true }
    }
  }
}
```

The scatter worker creates 3 copies, and the "process" step is invoked 3 times in parallel (once per copy).

## License

MIT
