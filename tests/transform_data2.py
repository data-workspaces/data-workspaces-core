#!/usr/bin/env python
# Test script to transform data

import sys
from os.path import exists
import pandas as pd

def transform(in_df):
    out_df = pd.DataFrame({'v1_sum':[in_df['v1'].sum()], 'v2_sum':[in_df['v2'].sum()]})
    return out_df

def main(argv=sys.argv[1:]):
    if len(argv)!=2:
        print("Wrong number of args.", file=sys.stderr)
        print("Usage: %s INPUT_FILE OUTPUT_FILE" % sys.argv[0], file=sys.stderr)
        return 1
    input_file = argv[0]
    if not exists(input_file):
        print("Input file %s does not exist." % input_file, file=sys.stderr)
        return 1
    output_file = argv[1]
    if input_file==output_file:
        print("Input file and output file should have different names.", file=sys.stderr)
        return 1
    in_df = pd.read_csv(input_file, header=0)
    out_df = transform(in_df)
    out_df.to_csv(output_file, index=False)
    print("File %s written successfully." % output_file)

if __name__=='__main__':
    sys.exit(main())
