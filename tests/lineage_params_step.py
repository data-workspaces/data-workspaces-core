#!/usr/bin/env python3
"""Testcase for issue #20
"""

import sys
import time
import os

from dataworkspaces.lineage import LineageBuilder

def main():
    force=1
    min_count = 5
    results_dir = './results'
    data_files=['./source-data/data.csv']
    output_csv = './intermediate-data'
    builder = (
        LineageBuilder()
        .as_script_step()
        .with_parameters({
            'force': force,
            'min_count': min_count
        })
        .as_results_step(os.path.join(results_dir, 'preprocessing/'),
                         run_description="this is a test")
    )

    builder = builder.with_input_paths(data_files)

    with builder.eval() as lineage:
        time.sleep(0.5)
        lineage.add_output_path(output_csv)
        lineage.write_results({
            'size_filtered': 45,
            'size_raw': 334
        })

    return 0

sys.exit(main())
