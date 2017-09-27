import sys
import json
spark_logs = sys.argv[1]
filteredLogs = sys.argv[2]
start_jobId = int(sys.argv[3])
end_jobId = int(sys.argv[4])

jsonLogs = []
for line in open(spark_logs):
	jsonLogs.append(json.loads(line))

outf = open(filteredLogs, "w")

write_line = True
for jlog in jsonLogs:
	if jlog["Event"] == "SparkListenerJobStart":
		if int(jlog["Job ID"]) < start_jobId or int(jlog["Job ID"]) > end_jobId:
			write_line = False
		else:
			write_line = True
	if write_line:
		outf.write(json.dumps(jlog) + "\n")
	if jlog["Event"] == "SparkListenerJobEnd":
		write_line = True
