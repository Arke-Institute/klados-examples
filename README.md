# Klados Examples

Example klados workers demonstrating different patterns for building agents on the Arke network.

## Examples

| Example | Description |
|---------|-------------|
| [stamp-worker](./stamp-worker) | Simple worker that stamps entities with metadata - great for learning the basics |

## Getting Started

### Prerequisites

- Node.js 18+
- Cloudflare account (for deploying workers)
- Arke user API key (`uk_...`)

### Quick Start

1. **Clone this repo**
   ```bash
   git clone https://github.com/Arke-Institute/klados-examples
   cd klados-examples
   ```

2. **Pick an example**
   ```bash
   cd stamp-worker
   npm install
   ```

3. **Configure your endpoint**

   Edit `agent.json` and update the endpoint URL to match your Cloudflare subdomain:
   ```json
   {
     "endpoint": "https://stamp-worker.YOUR-SUBDOMAIN.workers.dev"
   }
   ```

4. **Register with Arke**
   ```bash
   ARKE_USER_KEY=uk_... npm run register
   ```

5. **Run the tests**
   ```bash
   ARKE_USER_KEY=uk_... KLADOS_ID=<from-registration> npm test
   ```

## Creating Your Own Worker

These examples are based on the [klados-worker-template](https://github.com/Arke-Institute/klados-worker-template). To create a new worker:

```bash
git clone https://github.com/Arke-Institute/klados-worker-template my-worker
cd my-worker
npm install
```

See the template README for full documentation.

## Resources

- [klados-worker-template](https://github.com/Arke-Institute/klados-worker-template) - Base template for creating workers
- [@arke-institute/rhiza](https://www.npmjs.com/package/@arke-institute/rhiza) - Workflow protocol library
- [@arke-institute/klados-testing](https://www.npmjs.com/package/@arke-institute/klados-testing) - Testing utilities

## License

MIT
