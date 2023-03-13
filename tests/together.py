import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument("--a", type=str)
parser.add_argument("--b", type=str)
parser.add_argument("--c", type=str)
args = parser.parse_args()

time.sleep(1)

fn = "testfiles/file_h_{}_{}_{}".format(args.a, args.b, args.c)

f_ab = "testfiles/file_f_{}_{}".format(args.a, args.b)
g_ac = "testfiles/file_g_{}_{}".format(args.a, args.c)

with open(f_ab) as f:
    s1 = f.read()
with open(g_ac) as f:
    s2 = f.read()
with open(fn, "w") as f:
    f.write(s1 + " " + s2)
