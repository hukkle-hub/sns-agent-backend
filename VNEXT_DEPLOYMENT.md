# vNext staging deployment

- Confirmed Render source: `hukkle-hub/sns-agent-backend`, branch `main`, commit `767157a`.
- Rollback baseline: tag `pre-vnext-render-767157a`.
- Integration branch: `codex/vnext-render-integration`.
- Existing routes and existing DB fields are unchanged.
- New routes use `/api/vnext/*`; new state uses `DB.vnext`.
- Only `/api/vnext/health` is public. Every other vNext API follows the existing global authentication policy.

## Gate

1. Deploy the integration branch to a Render Preview or separate staging service.
2. Confirm `GET /api/vnext/health`.
3. Authenticate, then POST `sample-project.json` to `/api/vnext/projects`.
4. Confirm `/api/vnext/today?projectId=shorts-lab-0-to-500`.
5. Restart staging and repeat step 4 to verify persistence.
6. Regression-test existing projects, instructions, meetings, records, settings, publishing, Shorts, and schedules.
7. Promote only after every check passes.

## Rollback

Use Render's rollback control for commit `767157a`. Do not delete `DB.vnext`; the old server ignores it, so retaining it makes rollback safer and preserves staged vNext data.
