import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument("--a", type=str)
parser.add_argument("--b", type=str)
args = parser.parse_args()

time.sleep(1)

f1 = "tests/testfiles/file_f_{}_{}".format(args.a, args.b)
f2 = "tests/testfiles/file_ff_{}".format(args.a)

with open(f1, "w") as f:
    f.write("a: {} b: {}\n".format(args.a, args.b))
with open(f2, "w") as f:
    f.write("a: {} b: {}\n".format(args.a, args.b))
