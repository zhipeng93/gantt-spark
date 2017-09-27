import os
import sys

for line in open(sys.argv[1]):
	if line.startswith("#"):
		pass
	else:
		os.system("bash process.sh " + line.strip())
