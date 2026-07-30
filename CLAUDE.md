# tyi-take_yout_investiments

## Infra & deploy — READ THIS before any deploy / container / DB action

**Prod runs on the `vivobook` host, but you drive it from THIS repo dir.**
The local docker CLI is already pointed at prod via docker **context `note`**
(`ssh://victor@192.168.0.9`). So `docker`, `docker compose`, `docker exec`,
`docker logs` run here **target production directly**. Do NOT `ssh victor@192.168.0.9`
by hand, do NOT `sudo`, do NOT look for a checkout on the host — the build context
path on vivobook doesn't exist; builds **stream this working tree** over the ssh context.

**Deploy a service** (edit code here, then from repo root):
```bash
docker compose --profile <profile> build <service> && \
docker compose --profile <profile> up -d <service>
```
- `market-scanner` needs `--profile scanner`. `build` bakes the current working
  tree (no commit needed to deploy); `up -d` recreates the container with compose env.
- The **frontend** is a built Vite SPA — the served bundle lags the working tree
  until you rebuild+redeploy `frontend` (a code edit alone won't show live).

**Inspect / operate prod:** `docker exec <container> …` — containers: `backend`
(gunicorn, host :8001→8000), `frontend` (nginx SPA, :8000→80, proxies `/api/`→backend),
`market-scanner` (:8089, APScheduler), `pierre-service`, `openclaw-gateway`.
Live app: http://192.168.0.9:8000/.

**Database:** `backend` + `market-scanner` **share** `/app_vol/investments.db`
(host `/srv/tyi-take_yout_investiments/app_vol`, WAL mode). `sqlite3` CLI is **not**
installed in the containers — query via `docker exec <c> python3 -c "import sqlite3; …"`.
**Back up before destructive DB ops** (python `sqlite3` `.backup`).

**Accounts:** Victor uses login `amor` = **user id 3** (role trader). Scanner /
swing-trade trades are owned by uid 3 and tagged `[[TYI_UID:3]]` in `notes`
(the Swing Trade page filters by that marker).

## Memory
The assistant's durable notes live in the **auto-memory** loaded each session
(global path `~/.claude/projects/…/memory/`). The `.claude/memory/` directory
**inside this repo is a stale June-2026 snapshot and is NOT auto-loaded** — ignore it.
