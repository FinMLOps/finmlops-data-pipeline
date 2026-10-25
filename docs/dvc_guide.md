# DVC guide

We use [DVC](https://dvc.org) to version our data. Git keeps small pointer files (`data/raw.dvc`, `dvc.lock`), and DVC keeps the actual data in a cache and a remote. Every Git commit points at an exact version of the data, so we can always get back the data a result was built from.

## What's tracked

| File | What it is |
|---|---|
| `data/raw.dvc` | fingerprint of everything in `data/raw/` (all the raw API files) |
| `dvc.yaml` | the `preprocess` stage: reads `data/raw/`, `scripts/preprocess.py`, `scripts/config.py`, writes `data/processed/news.parquet` |
| `dvc.lock` | exact fingerprints of that stage's inputs and output the last time it ran |
| `.dvc/config` | shared DVC settings (committed) |
| `.dvc/config.local` | your own settings, like where your local remote lives (not committed) |

> [!NOTE]
> Airflow collects, validates and processes the data on its own schedule. DVC snapshots are something we take on purpose (with `make snapshot`), for example once a week or before we use the data for a report. In a production setup a separate deployment with a bot account could automate this.

## Everyday commands

**Take a snapshot** (record the current data as a new version, back it up, and commit the pointers):
```bash
make snapshot
```
That runs these four steps, which you can also run one by one:
```bash
uv run dvc add data/raw     # fingerprint the raw data
uv run dvc repro            # rebuild news.parquet, but only if raw data or the script changed
uv run dvc push             # copy new data to the remote
git commit -m "Data snapshot 2026-10-26" -- data/raw.dvc dvc.lock
```

> [!TIP]
> The `git commit -- data/raw.dvc dvc.lock` part only commits those two files, so anything else you have staged is left alone. Push it with your normal `git push`.

**Check what changed:**
```bash
uv run dvc status           # has data/raw or the stage changed since the last snapshot?
uv run dvc status -c        # is everything pushed to the remote?
```

**Go back to an old version of the data:**
```bash
git checkout <commit>       # any commit that has the data you want
uv run dvc checkout         # data/raw/ and news.parquet now match that commit
git checkout main && uv run dvc checkout   # back to the latest
```

**Get the data on a new machine** (once a shared remote like GCS is set up):
```bash
git clone https://github.com/FinMLOps/finmlops-data-pipeline.git
cd finmlops-data-pipeline
uv sync
uv run dvc pull
```

## Setup option 1: local remote (what we use for now)

A DVC remote doesn't have to be in the cloud, it can just be a folder on your machine. This is enough to version and restore data, but nobody else can pull from it.

DVC itself is already set up in the repo, so on a fresh clone you only need to pick a remote:
```bash
uv sync
uv run dvc remote add --local -d local ~/dvc-storage
uv run dvc push
```

> [!IMPORTANT]
> Use `--local` here. It saves the remote in `.dvc/config.local`, which isn't committed, because the folder path only exists on your machine. Without `--local` it would go into `.dvc/config` and point everyone else at a folder they don't have.

> [!CAUTION]
> A local remote is only a backup on the same machine. If your laptop dies, the data is gone. Switch to GCS (below) as soon as you can.

## Setup option 2: Google Cloud Storage (shared, for the team)

Once there's a bucket, everyone on the team can `dvc push` and `dvc pull` the same data.

> [!WARNING]
> Google Cloud is turned off for `@northeastern.edu` accounts, so you'll get "Google Cloud Platform service has been disabled" with your school account. Use a personal Gmail account for everything below.

### 1. Get a bucket

Either the person who owns the team's GCP project (for example `finmlops-team6`) adds you to it, or you create your own project with the course credits redeemed on your personal Gmail.

**If someone else owns the project**, ask them to give your personal Gmail access to the bucket: open the bucket, go to **Permissions**, then **Grant access**, add your email, and pick the role **Storage Object Admin**.

**If you're creating it yourself:**
1. Redeem the credits on your personal Gmail and check they show up under **Billing**.
2. Create a project (**IAM & Admin** then **Create a project**) linked to that billing account, and note the project ID.
3. Create the bucket (names are global, so it has to be unique):
   ```bash
   gcloud auth login                          # pick your personal Gmail
   gcloud config set project <project-id>
   gcloud storage buckets create gs://finmlops-dvc-<yourname> \
       --location=us-east1 --uniform-bucket-level-access
   ```

> [!TIP]
> Set a budget alert (**Billing**, then **Budgets & alerts**, for example $10) so a mistake elsewhere can't eat the credits. Our data costs pennies a month.

### 2. Log in so DVC can use it

DVC uses Google's "application default credentials", which is a separate login from `gcloud auth login`:
```bash
gcloud auth application-default login                       # same personal Gmail
gcloud auth application-default set-quota-project <project-id>
gcloud storage ls gs://<bucket>                              # no error = you have access
```

### 3. Point DVC at the bucket

This one does go in the shared `.dvc/config`, so everyone uses the same remote:
```bash
uv run dvc remote add -d gcs gs://<bucket>/dvc
uv run dvc remote remove --local local          # stop using the local folder as the default
uv run dvc push                                 # upload everything versioned so far
git add .dvc/config
git commit -m "Use GCS as the DVC remote"
```

After that, teammates only need step 2 (their own login, with access to the bucket) and `uv run dvc pull`.

## Troubleshooting

| Problem | Fix |
|---|---|
| `403` or "does not have storage.objects.list access" | the bucket permission is missing, or you're logged in with a different email than the one that was granted. Check with `gcloud auth list` and `gcloud auth application-default print-access-token` |
| "Google Cloud Platform service has been disabled" | you're using a `@northeastern.edu` account; switch to a personal Gmail |
| `dvc push` hangs at "Querying remote cache" | DVC can't find credentials. Run `gcloud auth application-default login` |
| "without a quota project" warning | `gcloud auth application-default set-quota-project <project-id>` |
| `make snapshot` says "nothing to commit" | the data hasn't changed since the last snapshot, so there's nothing new to record |
| `dvc checkout` says files are missing from the cache | the data was never pushed from the machine that made that snapshot; run `dvc push` there |
