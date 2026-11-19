# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time


def parse_statistics(text):
    signatures = {}
    for failed, runs, arguments in re.findall(
        r"Signature #\d+ failed (\d+) out of (\d+) times: "
        r"bloom_filter_agg\(\s*(.*?)\s*\) ->",
        text,
    ):
        arity = len(arguments.split(","))
        failed, runs = int(failed), int(runs)
        if failed > runs or arity in signatures:
            raise ValueError("Invalid or duplicate Bloom signature statistics")
        signatures[arity] = {
            "runs": runs,
            "error_iterations": failed,
            "non_error_native_iterations": runs - failed,
        }

    comparisons = {2: 0, 3: 0}
    for iteration in re.finditer(
        r"Started iteration (\d+).*?Done with iteration \1\b", text, re.DOTALL
    ):
        block = iteration.group()
        call = re.search(r'bloom_filter_agg\(ROW\["c0"\]((?:,\s*\d+){0,2})\)', block)
        if (
            call
            and "Query not supported in Spark" in block
            and re.search(r"Testing plan #[1-9]\d*\b", block)
        ):
            arity = 1 + call.group(1).count(",")
            if arity in comparisons:
                comparisons[arity] += 1

    verified = re.findall(
        r"Total iterations verified against reference DB:\s*(\d+)", text
    )
    return {
        "bloom_signatures": signatures,
        "successful_nontrivial_native_iterations": comparisons,
        "reference_verified_iterations": int(verified[-1]) if verified else 0,
    }


def validate_coverage(statistics, campaign):
    for arity in (2, 3):
        signature = statistics["bloom_signatures"].get(arity)
        if not signature or signature["non_error_native_iterations"] <= 0:
            raise ValueError(f"No non-error native results for Bloom arity {arity}")
        if statistics["successful_nontrivial_native_iterations"][arity] <= 0:
            raise ValueError(
                f"No nontrivial native comparisons for Bloom arity {arity}"
            )
    if campaign == "mixed" and statistics["reference_verified_iterations"] <= 0:
        raise ValueError("Mixed campaign did not verify any reference queries")


def run_campaign(arguments):
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if revision != arguments.expected_revision:
        raise ValueError("Source revision does not match the requested PR head")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("Source checkout contains changes")
    binary = arguments.binary.resolve(strict=True)
    output = arguments.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / "repro").mkdir()
    command = [
        str(binary),
        f"--seed={arguments.seed}",
        "--steps=0",
        "--duration_sec=1800",
        "--enable_oom_injection=false",
        "--log_signature_stats=true",
        "--logtostderr=true",
        "--minloglevel=0",
        f"--repro_persist_path={output / 'repro'}",
        f"--table_name_prefix={arguments.campaign}",
        "--allocator_capacity=4294967296",
        "--arbitrator_capacity=2147483648",
    ]
    if arguments.campaign == "focused":
        command.append("--only=bloom_filter_agg")
    else:
        command.extend(
            [
                "--enable_sorted_aggregations=false",
                "--enable_streaming_aggregations=false",
            ]
        )
    record = {
        "source_revision": revision,
        "harness_revision": os.environ.get("GITHUB_SHA"),
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID"),
        "workflow_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "campaign": arguments.campaign,
        "seed": arguments.seed,
        "requested_duration_sec": 1800,
        "command": command,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "status": "running",
    }
    record_path = output / "run.json"
    with record_path.open("x") as stream:
        json.dump(record, stream, indent=2)
    print(json.dumps(record, indent=2), flush=True)
    started = time.monotonic()
    with (output / "stdout.log").open("x") as stream:
        try:
            result = subprocess.run(
                command, stdout=stream, stderr=subprocess.STDOUT, timeout=2250
            )
            returncode = result.returncode
        except subprocess.TimeoutExpired:
            returncode = 124
            stream.write("\nERROR: Campaign exceeded its 2250-second timeout\n")
    record.update(
        returncode=returncode,
        elapsed_sec=time.monotonic() - started,
        finished_utc=datetime.now(timezone.utc).isoformat(),
        status="completed" if returncode == 0 else "failed",
    )
    record_path.write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record, indent=2), flush=True)
    if returncode != 0:
        print("\n".join((output / "stdout.log").read_text().splitlines()[-50:]))
        raise SystemExit(f"{arguments.campaign} campaign failed: exit {returncode}")


def summarize(arguments):
    campaigns = {}
    hashes = set()
    errors = []
    for campaign in ("focused", "mixed"):
        directory = arguments.output / campaign
        record = json.loads((directory / "run.json").read_text())
        statistics = parse_statistics((directory / "stdout.log").read_text())
        campaigns[campaign] = {"run": record, "statistics": statistics}
        hashes.add(record["binary_sha256"])
        if record["source_revision"] != arguments.expected_revision:
            errors.append(f"{campaign}: source revision mismatch")
        if record["status"] != "completed" or record["elapsed_sec"] < 1800:
            errors.append(
                f"{campaign}: campaign failed or ran for less than 1800 seconds"
            )
        try:
            validate_coverage(statistics, campaign)
        except ValueError as error:
            errors.append(f"{campaign}: {error}")
    if len(hashes) != 1:
        errors.append("Campaigns used different binaries")
    summary = {
        "source_revision": arguments.expected_revision,
        "campaigns": campaigns,
        "errors": errors,
        "qualification": (
            "Bloom comparisons are between native Velox plans, not Spark results. "
            "Nontrivial counts require a successful native baseline, a later plan, "
            "and completed iteration, with OOM injection disabled."
        ),
    }
    (arguments.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    if errors:
        raise SystemExit("\n".join(errors))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("run", "summarize"))
    parser.add_argument("--expected-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--campaign", choices=("focused", "mixed"))
    parser.add_argument("--seed", type=int)
    arguments = parser.parse_args()
    if arguments.action == "run":
        if (
            arguments.binary is None
            or arguments.campaign is None
            or arguments.seed is None
        ):
            parser.error("run requires --binary, --campaign and --seed")
        run_campaign(arguments)
    else:
        summarize(arguments)


if __name__ == "__main__":
    main()
