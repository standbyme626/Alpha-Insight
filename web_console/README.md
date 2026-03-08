# Upgrade7 Next Console

Reference-based frontend shell for Upgrade7 P1.

## Reference Mapping

- `next-shadcn-dashboard-starter`: dashboard shell and sidebar navigation layout.
- `nextjs-fastapi-template`: typed client pattern for resource fetching.
- `react-admin` / `refine`: resource-first route model (`runs`, `alerts`, `evidence`, `governance`).

## Run

```bash
cd /home/kkk/Project/Alpha-Insight
PYTHONPATH=. python services/resource_api.py --port 8765
cd web_console
npm install
npm run dev
```

Open `http://localhost:8600`.

Optional compatibility snapshot export (evidence/backfill only):

```bash
PYTHONPATH=. python scripts/upgrade7_frontend_resources_export.py
```
