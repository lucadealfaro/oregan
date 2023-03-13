import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument("--a", type=str)
parser.add_argument("--c", type=str)
args = parser.parse_args()

time.sleep(1)

fn = "tests/testfiles/file_g_{}_{}".format(args.a, args.c)

with open(fn, "w") as f:
    f.write("done")
