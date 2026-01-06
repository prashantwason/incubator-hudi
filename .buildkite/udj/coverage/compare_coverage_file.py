#!/usr/bin/env python3

import xml.etree.ElementTree as ET
import sys
import subprocess
import os

def parse_source_coverage(file_path):
    """Parses the JaCoCo XML report and extracts coverage data at the source file level using <counter>."""
    coverage_data = {}

    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        for package in root.findall(".//package"):
            package_name = package.get("name", "NO_PACKAGE")

            for source in package.findall("sourcefile"):
                source_name = source.get("name")
                full_path = "{}/{}".format(package_name, source_name)

                # Extract line coverage from <counter type="LINE">
                counter = source.find(".//counter[@type='LINE']")
                if counter is not None:
                    missed = int(counter.get("missed", 0))
                    covered = int(counter.get("covered", 0))
                    total = missed + covered
                    coverage = (covered / float(total)) * 100 if total > 0 else 0

                    # Store the file coverage data
                    coverage_data[full_path] = {
                        "missed": missed,
                        "covered": covered,
                        "total": total,
                        "coverage": coverage
                    }


        # Extract overall coverage from root counter
        overall_counter = root.find("counter[@type='LINE']")

        if overall_counter is not None:
            total_missed = int(overall_counter.get("missed", 0))
            total_covered = int(overall_counter.get("covered", 0))
            total_lines = total_missed + total_covered
            overall_coverage = (total_covered / float(total_lines)) * 100 if total_lines > 0 else 0

            # Add overall coverage stats to the coverage data
            coverage_data["overall_coverage"] = {
                "covered": total_covered,
                "missed": total_missed,
                "total": total_lines,
                "coverage_percent": overall_coverage
            }
        else:
            raise Exception("Could not find overall LINE counter in the XML file: {}".format(file_path))

    except Exception as e:
        raise Exception("Error parsing file {}: {}".format(file_path, str(e)))

    return coverage_data

def compare_file_coverage(base_coverage, changed_coverage):
    """Compares source file-level coverage between base and changed reports."""
    try:
        with open("build/comment/phabricator-comment-code-coverage/newline_coverage.md", "w") as f:
            f.write("\n## Code Coverage Report\n\n")

            base_overall = base_coverage.get("overall_coverage", {}).get("coverage_percent", 0)
            changed_overall = changed_coverage.get("overall_coverage", {}).get("coverage_percent", 0)

            # Add warning if coverage decreased
            if changed_overall < base_overall:
                f.write(f"> ⚠️ **Warning**: Coverage has decreased by {base_overall - changed_overall:.2f}%\n\n")

            f.write("### Coverage Summary\n\n")
            f.write(f"- **Base Coverage**: {base_overall:.2f}%\n")
            f.write(f"- **Current Coverage**: {changed_overall:.2f}%\n")
            f.write(f"- **Change**: {changed_overall - base_overall:+.2f}%\n\n")

            f.write("### Detailed File Changes\n\n")
            f.write("| Source File | Previous Coverage % | Current Coverage % | Delta % | Status |\n")
            f.write("|-------------|----------------------|--------------------|---------|--------|\n")

            all_files = set(base_coverage.keys()).union(set(changed_coverage.keys()))

            for file in sorted(all_files):
                base_data = base_coverage.get(file, {"coverage": 0})
                changed_data = changed_coverage.get(file, {"coverage": 0})

                base_coverage_pct = base_data.get("coverage", 0)
                changed_coverage_pct = changed_data.get("coverage", 0)
                delta = changed_coverage_pct - base_coverage_pct

                if file in base_coverage and file in changed_coverage:
                    status = "Changed" if delta != 0 else "Unchanged"
                elif file in base_coverage:
                    status = "Removed"
                else:
                    status = "Added"

                if status != "Unchanged":
                    f.write(f"| `{file}` | {base_coverage_pct:.2f}% | {changed_coverage_pct:.2f}% | {delta:.2f}% | {status} | \n")

    except Exception as e:
        with open("build/comment/phabricator-comment-code-coverage/newline_coverage.md", "a") as f:
            f.write("\n> :warning: The comparison tool ran into an error and the report may be incomplete.\n")
            f.write(f"> Error details: {str(e)}\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare_coverage_file.py <base_report.xml> <changed_report.xml>")
        sys.exit(1)

    base_file = sys.argv[1]
    changed_file = sys.argv[2]

    try:
        # Parse file-level coverage
        base_coverage = parse_source_coverage(base_file)
        changed_coverage = parse_source_coverage(changed_file)

        # Compare file-level coverage
        compare_file_coverage(base_coverage, changed_coverage)
    except Exception as e:
        sys.exit(1)
