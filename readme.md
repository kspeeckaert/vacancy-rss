# vacancy-rss

This repository collects job vacancies from three Belgian job sites and publishes them as feeds. A GitHub Actions workflow updates the feeds every two hours.

## Feeds

The feeds use the Atom format. Most feed readers that support RSS also read Atom.

| Source | Feed URL | Content |
|---|---|---|
| [VDAB](https://www.vdab.be) | `https://raw.githubusercontent.com/kspeeckaert/vacancy-rss/main/feeds/vdab.xml` | Vacancies that match the search in `config/vdab.json` |
| [editx](https://editx.eu) | `https://raw.githubusercontent.com/kspeeckaert/vacancy-rss/main/feeds/editx.xml` | IT vacancies that match the search in `config/editx.json` |
| [Cartière](https://www.cartiere.be) | `https://raw.githubusercontent.com/kspeeckaert/vacancy-rss/main/feeds/cartiere.xml` | All vacancies on the site, except freelance vacancies |

## How the update works

The `Orchestrator` workflow runs every two hours from 04:00 to 22:00 UTC. It starts one job for each feed. Each job downloads the vacancies, writes the feed and uploads it as a workflow artifact. A workflow artifact is a file that the jobs in one run can share.

A final job collects the feeds and commits them to `main` in one commit. If only the feed timestamp changed, the job makes no commit.

If one feed job fails, the final job still publishes the other feeds. If a script cannot produce a feed with at least one entry, the script fails. The existing feed file then stays unchanged.

If the title, ID or link of a vacancy is missing, the script skips that vacancy. If an optional field is missing, the entry stays in the feed without that field.

## Run a script locally

You need [uv](https://docs.astral.sh/uv/). uv installs Python 3.13 from `.python-version` and the exact package versions from `uv.lock`.

1. Install the packages:

   ```zsh
   uv sync
   ```

2. Run the script for the feed that you want:

   ```zsh
   uv run python vdab.py -d -p 20 -v feeds/vdab.xml config/vdab.json
   uv run python editx.py -d -v feeds/editx.xml config/editx.json
   uv run python cartiere.py -v feeds/cartiere.xml
   ```

The scripts accept these options:

| Option | Scripts | Effect |
|---|---|---|
| `-d`, `--details` | `vdab.py`, `editx.py` | Downloads the full description of each vacancy. `cartiere.py` always does this. |
| `-p`, `--page_count` | `vdab.py` | Sets the maximum number of search pages. The default is 20. |
| `-v`, `--verbose` | All | Writes debug messages to the log. |

The scripts wait 3 to 10 seconds between two requests for vacancy details. A full run of `vdab.py` can take more than 20 minutes.

## Change a search

The file `config/vdab.json` contains the request body that the VDAB website sends for a search. The value `paginaGrootte` sets the number of results on each page. With 20 pages of 10 results, the feed holds at most 200 vacancies.

The file `config/editx.json` contains the query parameters for the editx job search. The value `count` sets the number of results on each page. `editx.py` downloads all pages.

Cartière has no search configuration. `cartiere.py` reads all vacancies from the sitemap of the site.

## Change the dependencies

1. Edit the dependencies in `pyproject.toml`.
2. Run `uv lock` to update `uv.lock`.
3. Commit `pyproject.toml` and `uv.lock` together.

To update all packages to their latest versions, run `uv lock --upgrade`.

If `uv.lock` does not match `pyproject.toml`, the workflow jobs fail, because they install with `uv sync --locked`.

## Code structure

Each site has its own script: `vdab.py`, `editx.py` and `cartiere.py`. A script contains the code that reads the data of its site and turns it into feed entries.

The file `common.py` contains the code that the three scripts share:

- The HTTP session, with a timeout and retries for each request.
- The pause between two detail requests.
- The feed header, the entry with its required fields, and the empty-feed guard.
- The logging setup and the exit code for the workflow.

## Versions

The project uses the version format `major.minor.date`, for example `0.4.20261012`. The version is in `pyproject.toml` and in the `__version__` value of each script and of `common.py`.
