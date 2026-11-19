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
import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts.ci.pr20127_evidence import (
    parse_statistics,
    run_campaign,
    summarize,
    validate_coverage,
)


COMPLETED_LOG = """
Started iteration 7 (seed: 123)
bloom_filter_agg(ROW["c0"],128)
Query not supported in Spark
Testing plan #1
Done with iteration 7
Started iteration 8 (seed: 456)
bloom_filter_agg(ROW["c0"],128,1024)
Query not supported in Spark
Testing plan #2
Done with iteration 8
Signature #1 failed 2 out of 5 times: bloom_filter_agg(bigint, constant bigint) -> varbinary
Signature #2 failed 0 out of 3 times: bloom_filter_agg(bigint, constant bigint, constant bigint) -> varbinary
Total iterations verified against reference DB: 12 (10%)
"""


class Pr20127EvidenceTest(unittest.TestCase):
    def test_completed_comparisons_and_error_iterations(self):
        statistics = parse_statistics(COMPLETED_LOG)
        self.assertEqual(
            statistics["bloom_signatures"][2]["non_error_native_iterations"], 3
        )
        self.assertEqual(
            statistics["successful_nontrivial_native_iterations"], {2: 1, 3: 1}
        )
        validate_coverage(statistics, "mixed")

    def test_selection_baseline_rerun_and_incomplete_iteration_are_not_coverage(self):
        for markers in (
            "Testing plan #1\nDone with iteration 7",
            "Query not supported in Spark\nTesting plan #0\nDone with iteration 7",
            "Query not supported in Spark\nTesting plan #1",
        ):
            with self.subTest(markers=markers):
                statistics = parse_statistics(
                    'Started iteration 7 (seed: 123)\nbloom_filter_agg(ROW["c0"],128)\n'
                    + markers
                )
                self.assertEqual(
                    statistics["successful_nontrivial_native_iterations"], {2: 0, 3: 0}
                )
                with self.assertRaisesRegex(ValueError, "No non-error native results"):
                    validate_coverage(statistics, "focused")

    def test_all_error_signatures_and_skipped_reference_queries_fail(self):
        statistics = {
            "bloom_signatures": {
                2: {"non_error_native_iterations": 0},
                3: {"non_error_native_iterations": 1},
            },
            "successful_nontrivial_native_iterations": {2: 1, 3: 1},
            "reference_verified_iterations": 0,
        }
        with self.assertRaisesRegex(ValueError, "No non-error native results"):
            validate_coverage(statistics, "focused")
        statistics["bloom_signatures"][2]["non_error_native_iterations"] = 1
        with self.assertRaisesRegex(ValueError, "did not verify any reference queries"):
            validate_coverage(statistics, "mixed")
        statistics["successful_nontrivial_native_iterations"][3] = 0
        with self.assertRaisesRegex(ValueError, "No nontrivial native comparisons"):
            validate_coverage(statistics, "focused")

    def test_failed_or_timed_out_runs_preserve_failure_records(self):
        for outcome in (
            subprocess.CompletedProcess([], 1),
            subprocess.TimeoutExpired([], 2250),
        ):
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binary = root / "binary"
                binary.write_bytes(b"synthetic test binary; never executed")
                arguments = argparse.Namespace(
                    expected_revision="revision",
                    binary=binary,
                    output=root / "focused",
                    campaign="focused",
                    seed=123,
                )
                with (
                    mock.patch(
                        "scripts.ci.pr20127_evidence.subprocess.check_output",
                        side_effect=["revision\n", ""],
                    ),
                    mock.patch(
                        "scripts.ci.pr20127_evidence.subprocess.run",
                        **(
                            {"side_effect": outcome}
                            if isinstance(outcome, Exception)
                            else {"return_value": outcome}
                        ),
                    ),
                    contextlib.redirect_stdout(io.StringIO()),
                    self.assertRaisesRegex(SystemExit, "campaign failed"),
                ):
                    run_campaign(arguments)
                record = json.loads((arguments.output / "run.json").read_text())
                self.assertEqual(record["status"], "failed")
                self.assertNotEqual(record["returncode"], 0)

    def test_short_campaign_cannot_be_reported_as_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for campaign, elapsed in (("focused", 1799), ("mixed", 1801)):
                output = root / campaign
                output.mkdir()
                (output / "stdout.log").write_text(COMPLETED_LOG)
                (output / "run.json").write_text(
                    json.dumps(
                        {
                            "binary_sha256": "same-binary",
                            "source_revision": "revision",
                            "status": "completed",
                            "elapsed_sec": elapsed,
                        }
                    )
                )
            with self.assertRaisesRegex(SystemExit, "less than 1800 seconds"):
                summarize(
                    argparse.Namespace(output=root, expected_revision="revision")
                )
            summary = json.loads((root / "summary.json").read_text())
            self.assertTrue(summary["errors"])


if __name__ == "__main__":
    unittest.main()
