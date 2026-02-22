# Rhiza Workflow Template

A template for testing rhiza workflow compositions on the Arke network.

## Overview

This template provides the infrastructure to:
- Define workflow compositions (JSON)
- Register workflows as rhiza entities on Arke
- Test workflow execution end-to-end
- Verify data flows correctly through workflow steps

**Key concept**: Rhizas reference klados workers that exist on the network. Workers are independent, portable entities - this template focuses on testing how they compose together.

## Quick Start

### 1. Clone this template

```bash
git clone https://github.com/Arke-Institute/rhiza-workflow-template my-workflow
cd my-workflow
npm install
```

### 2. Set up your environment

Copy the example environment file:
```bash
cp .env.example .env
```

Edit `.env` with your configuration:
```bash
ARKE_USER_KEY=uk_your_key_here
ARKE_NETWORK=test

# Klados IDs (workers must be deployed and active)
# Note: Same klados can be used in multiple steps
STAMP_KLADOS=klados_your_stamp_worker
```

### 3. Find available klados workers

List klados workers you have access to:
```bash
npm run list-kladoi
```

### 4. Register your workflow

```bash
npm run register -- stamp-chain
```

This creates a rhiza entity on Arke and saves its ID to `.rhiza-state.json`.

### 5. Run the tests

```bash
RHIZA_ID=rhiza_xxx npm test
```

## Project Structure

```
rhiza-workflow-template/
├── workflows/               # Workflow definitions (JSON)
│   └── stamp-chain.json     # Example: stamp → stamp chain
├── scripts/
│   ├── register.ts          # Create rhiza from definition
│   └── list-kladoi.ts       # List available workers
├── test/
│   ├── setup.ts             # Shared test configuration
│   └── stamp-chain.test.ts  # Workflow tests
├── package.json
├── vitest.config.ts
└── .env.example
```

## Workflow Definition Format

Workflows use a **step-based format** where:
- `entry` is a step name (string)
- `flow` keys are step names (not klados IDs)
- Each step has `klados` (which worker to invoke) and `then` (what happens after)
- The same klados can appear in multiple steps

```json
{
  "label": "My Workflow",
  "description": "What this workflow does",
  "version": "2.0",
  "entry": "first_step",
  "flow": {
    "first_step": {
      "klados": { "pi": "$KLADOS_ID" },
      "then": { "pass": "second_step" }
    },
    "second_step": {
      "klados": { "pi": "$KLADOS_ID" },
      "then": { "done": true }
    }
  }
}
```

**Variable substitution**: Use `$ENV_VAR` syntax for klados references. The registration script substitutes these from environment variables.

### Handoff Types

Each step's `then` specifies what happens after the klados completes:

- `{ "pass": "step_name" }` - Pass output to next step (1:1)
- `{ "scatter": "step_name" }` - Fan out to multiple parallel invocations (1:N)
- `{ "gather": "step_name" }` - Collect outputs from scatter (N:1)
- `{ "done": true }` - Workflow complete

### Why Step Names?

Using step names (instead of klados IDs as keys) allows:
1. The same klados to appear multiple times in a workflow
2. Clear naming that describes what each step does
3. Path tracking that shows execution history

## Creating Your Own Workflow

1. **Create a workflow definition** in `workflows/my-workflow.json`
2. **Set environment variables** for the klados IDs used
3. **Register**: `npm run register -- my-workflow`
4. **Create test file** in `test/my-workflow.test.ts`
5. **Run tests**: `npm test`

## Example: Stamp Chain

The included `stamp-chain` workflow demonstrates using the same klados twice:

```
Entity → Stamp Worker (first_stamp) → Stamp Worker (second_stamp) → Done
                ↓                              ↓
           adds stamp[0]                  adds stamp[1]
```

Both steps use the same klados (`$STAMP_KLADOS`), but each step has a unique name. The workflow path tracks which step was executed: `["first_stamp"]` then `["first_stamp", "second_stamp"]`.

After the workflow completes, the entity has:
```json
{
  "stamps": [
    { "stamp_number": 1, "stamped_by": "klados_stamp", ... },
    { "stamp_number": 2, "stamped_by": "klados_stamp", ... }
  ],
  "stamp_count": 2
}
```

## Testing Utilities

This template uses `@arke-institute/klados-testing` which provides:

- `invokeRhiza()` - Start a workflow
- `waitForWorkflowCompletion()` - Poll until all steps complete
- `assertWorkflowCompleted()` - Verify workflow success
- `createCollection()`, `createEntity()`, `deleteEntity()` - Test fixtures

## Environment Variables

| Variable | Description |
|----------|-------------|
| `ARKE_API_BASE` | API URL (default: https://arke-v1.arke.institute) |
| `ARKE_USER_KEY` | Your user API key (uk_...) |
| `ARKE_NETWORK` | Network: 'test' or 'main' |
| `RHIZA_ID` | Rhiza ID (from registration) |
| `STAMP_KLADOS` | Stamp worker ID |

## Resources

- [klados-examples](https://github.com/Arke-Institute/klados-examples) - Example klados workers
- [klados-worker-template](https://github.com/Arke-Institute/klados-worker-template) - Create new workers
- [@arke-institute/rhiza](https://www.npmjs.com/package/@arke-institute/rhiza) - Workflow protocol library
- [@arke-institute/klados-testing](https://www.npmjs.com/package/@arke-institute/klados-testing) - Testing utilities

## License

MIT
