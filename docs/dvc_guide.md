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

**Get the data on a new machine:**
```bash
git clone https://github.com/FinMLOps/finmlops-data-pipeline.git
cd finmlops-data-pipeline
uv sync
gcloud auth application-default login   # the account that has access to the bucket
uv run dvc pull
```

## Setup: our GCS bucket (what we use)

Our DVC remote is a Google Cloud Storage bucket in our teammate Kshitij's personal Google Cloud account. We tried the Google Cloud access that came with our university accounts first, but it has a lot of settings and restrictions (for example, we couldn't create projects), so a personal account was much simpler.

| | |
|---|---|
| Bucket | `finmlops-pipeline-dvc` (project `finmlops-pipeline`, region `us-east1`) |
| DVC remote | `gcs` → `gs://finmlops-pipeline-dvc/dvc`, already the default in `.dvc/config` |
| Access | Storage Object Admin on this bucket only, given to each of us by email |

The remote is already set up in the repo, so you only need access and a login:

1. Ask Kshitij to add your email to the bucket (bucket → **Permissions** → **Grant access** → **Storage Object Admin**).
2. Log in so DVC can use it. DVC uses Google's "application default credentials", which is a separate login from `gcloud auth login`:
   ```bash
   gcloud auth application-default login            # pick the email that was given access
   gcloud storage ls gs://finmlops-pipeline-dvc     # no error = you have access
   ```
3. Run `uv run dvc pull` to get the data, or `uv run dvc push` after a snapshot.

## Optional: a local backup remote

Before we had the bucket we used a folder on our own machine as the remote. You can still keep one as an extra backup:
```bash
uv run dvc remote add --local local ~/dvc-storage
uv run dvc push -r local
```

> [!IMPORTANT]
> Use `--local` so it goes into `.dvc/config.local`, which isn't committed (the folder only exists on your machine), and **don't** add `-d`. A default set in `.dvc/config.local` overrides the shared one, so you'd quietly stop pushing to the bucket. If that happens, run `uv run dvc remote default --local --unset`.

## Not on our team? Use your own bucket

Our bucket is private, so to run the pipeline somewhere else, make your own bucket and point DVC at it.

> [!WARNING]
> University Google accounts can come with restrictions (ours gave "Google Cloud Platform service has been disabled" when we tried to create a project). A personal Google account is the easiest way.

### 1. Make a bucket

1. Set up billing on your Google account (free trial or credits) and check it shows up under **Billing**.
2. Create a project (**IAM & Admin** then **Create a project**) linked to that billing account, and note the project ID.
3. Create the bucket (names are global, so it has to be unique):
   ```bash
   gcloud auth login
   gcloud config set project <project-id>
   gcloud storage buckets create gs://<your-bucket> \
       --location=us-east1 --uniform-bucket-level-access
   ```

> [!TIP]
> Set a budget alert (**Billing**, then **Budgets & alerts**, for example $10) so a mistake elsewhere can't eat the credits. Our data costs pennies a month.

### 2. Log in so DVC can use it

```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project <project-id>
gcloud storage ls gs://<your-bucket>                 # no error = you have access
```

### 3. Point DVC at your bucket

```bash
uv run dvc remote modify gcs url gs://<your-bucket>/dvc
uv run dvc push                                      # upload everything you've versioned
```

## Troubleshooting

| Problem | Fix |
|---|---|
| `403` or "does not have storage.objects.list access" | the bucket permission is missing, or you're logged in with a different email than the one that was given access. Check with `gcloud auth list` and run `gcloud auth application-default login` again with the right one |
| "Google Cloud Platform service has been disabled" | your account (usually a university one) isn't allowed to use Google Cloud this way. Ask for access to our bucket instead, or use a personal Google account |
| `dvc push` hangs at "Querying remote cache" | DVC can't find credentials. Run `gcloud auth application-default login` |
| "without a quota project" warning | harmless, reads and writes are billed to the bucket's project. To hide it: `gcloud auth application-default set-quota-project finmlops-pipeline` (needs access to the project, not just the bucket, so it may fail; that's fine) |
| `dvc push` goes to `~/dvc-storage` and not the bucket | `.dvc/config.local` has its own default. Run `uv run dvc remote default --local --unset` |
| `make snapshot` says "nothing to commit" | the data hasn't changed since the last snapshot, so there's nothing new to record |
| `dvc checkout` says files are missing from the cache | the data was never pushed from the machine that made that snapshot; run `dvc push` there |
