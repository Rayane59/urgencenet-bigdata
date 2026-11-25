from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from time import monotonic, sleep
from urllib.parse import quote
from urllib.request import urlopen

from playwright.sync_api import Page, expect, sync_playwright

MASTER = "http://127.0.0.1:8080"
HISTORY = "http://127.0.0.1:18080"
MINIO = "http://127.0.0.1:9001"


def get_json(url):
    with urlopen(url, timeout=5) as response:
        return json.load(response)


def get_batch_log():
    for path in sorted(Path("spark-events").glob("app-*"), reverse=True):
        if not path.is_file() or path.suffix or path.name.startswith("."):
            continue
        events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        if any((event.get("Properties") or {}).get("spark.jobGroup.id") == "RDD" for event in events):
            return path.name, events
    raise RuntimeError("Journal de l’application batch introuvable.")


def choose_jobs(events):
    starts = [event for event in events if event["Event"] == "SparkListenerJobStart"]
    completed = {
        event["Job ID"] for event in events
        if event["Event"] == "SparkListenerJobEnd" and event["Job Result"]["Result"] == "JobSucceeded"
    }
    rdd_job = next(event for event in starts if event["Job ID"] in completed
                   and (event.get("Properties") or {}).get("spark.jobGroup.id") == "RDD")

    sql_candidates = [
        event for event in starts if event["Job ID"] in completed
        and (event.get("Properties") or {}).get("spark.jobGroup.id") == "SQL"
        and "spark.sql.execution.id" in event["Properties"]
    ]
    matching_plans = {
        str(event["executionId"]) for event in events
        if event["Event"].endswith("SparkListenerSQLExecutionStart")
        and "BroadcastHashJoin" in event.get("physicalPlanDescription", "")
        and "Left keys [1]: [facility_id#" in event.get("physicalPlanDescription", "")
    }
    join_jobs = [
        event for event in sql_candidates
        if event["Properties"]["spark.sql.execution.id"] in matching_plans
    ]
    sql_job = max(join_jobs, key=lambda event: (len(event["Stage IDs"]), -event["Job ID"]))
    return rdd_job, sql_job


def wait_for_history(application):
    url = f"{HISTORY}/api/v1/applications/{quote(application)}/jobs"
    deadline = monotonic() + 120
    while monotonic() < deadline:
        try:
            jobs = get_json(url)
            if jobs:
                return
        except OSError:
            pass
        sleep(2)
    raise RuntimeError(f"Historique Spark indisponible pour {application}.")


def capture_job(page: Page, application: str, job: dict, path: Path):
    page.goto(f"{HISTORY}/history/{quote(application)}/jobs/job/?id={job['Job ID']}")
    graph = page.locator("#dag-viz-graph svg")
    if not graph.is_visible():
        page.locator("#job-dag-viz").click()
    expect(graph).to_be_visible(timeout=15000)
    expect(page.get_by_text(re.compile(r"Completed Stages")).first).to_be_visible()
    page.screenshot(path=path, full_page=True, animations="disabled")


def capture_minio(page: Page, output: Path):
    page.goto(MINIO)
    page.locator('input:visible').first.fill("urgencenet")
    page.locator('input[type=password]').fill("urgency-demo-local")
    page.get_by_role("button", name=re.compile(r"^(log|sign)\s*in$", re.I)).click()
    page.wait_for_url(re.compile(r".*/(browser|buckets|dashboard).*"), timeout=15000)
    page.goto(f"{MINIO}/browser/lake")
    expect(page.get_by_text("bronze", exact=True).first).to_be_visible(timeout=15000)
    expect(page.get_by_text("silver", exact=True).first).to_be_visible(timeout=15000)
    page.screenshot(path=output / "minio-bucket-lake.png", full_page=True, animations="disabled")


def main():
    parser = argparse.ArgumentParser(description="Capturer les preuves d’une exécution du jalon 1.")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    output = args.output
    output.mkdir(exist_ok=True, parents=True)

    master_state = get_json(f"{MASTER}/json/")
    assert len([worker for worker in master_state["workers"] if worker["state"] == "ALIVE"]) == 2
    application, events = get_batch_log()
    rdd_job, sql_job = choose_jobs(events)
    wait_for_history(application)
    app_url = f"{HISTORY}/history/{quote(application)}"
    jobs = get_json(f"{HISTORY}/api/v1/applications/{quote(application)}/jobs")
    selected = {job["Job ID"] for job in (rdd_job, sql_job)}
    (output / "jobs-et-stages.json").write_text(
        json.dumps([job for job in jobs if job["jobId"] in selected], indent=2), encoding="utf-8"
    )

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel=os.environ.get("PLAYWRIGHT_CHANNEL"))
        page = browser.new_page(viewport={"width": 1600, "height": 1080})
        page.goto(MASTER)
        expect(page.get_by_text(re.compile(r"Alive Workers")).first).to_be_visible()
        page.screenshot(path=output / "spark-master-deux-workers.png", full_page=True, animations="disabled")

        capture_job(page, application, rdd_job, output / "spark-rdd-dag-et-stages.png")
        capture_job(page, application, sql_job, output / "spark-sql-dag-et-stages.png")
        sql_id = sql_job["Properties"]["spark.sql.execution.id"]
        page.goto(f"{app_url}/SQL/execution/?id={sql_id}")
        expect(page.locator("#plan-viz-graph svg")).to_be_visible(timeout=15000)
        page.screenshot(path=output / "spark-sql-plan-jointure.png", full_page=True, animations="disabled")
        capture_minio(page, output)
        browser.close()


if __name__ == "__main__":
    main()
