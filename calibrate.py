import json
import sys

infile = sys.argv[1]
executor_num = int(sys.argv[2])

# assume executor_time = driver_time + x, let's get the range of x
class Executor_Delta:
	def __init__(self):
		self.min_x = None
		self.max_x = None

	def update_max(self, executor_minus_driver_time):
		if self.max_x == None:
			self.max_x = executor_minus_driver_time
		else:
			self.max_x = min(self.max_x, executor_minus_driver_time)
		if not self.min_x is None:
			if self.min_x > self.max_x:
				print "xx", self.min_x, self.max_x

	def update_min(self, executor_minus_driver_time):
		if self.min_x == None:
			self.min_x = executor_minus_driver_time
		else:
			self.min_x = max(executor_minus_driver_time, self.min_x)
		if not self.max_x is None:
			if self.min_x > self.max_x:
				print "xx", self.min_x, self.max_x


exe_delta = [Executor_Delta() for i in range(executor_num + 1)]


for line in open(infile):
	jlog = json.loads(line)
	eventType = jlog["Event"]
	if eventType == "SparkListenerTaskEnd":
		executor_id = int(jlog["Task Info"]["Executor ID"])

		driver_send_task_desc_ts = float(jlog["driverSendTaskDescTime"])
		executor_get_task_desc_ts = float(jlog["ExecutorGetTaskDescTime"])
		exe_delta[executor_id].update_max(executor_get_task_desc_ts - driver_send_task_desc_ts)
		# if executor_id == 1:
		# 	print "max", (executor_get_task_desc_ts - driver_send_task_desc_ts)

		task_send_result_via_rpc_ts = float(jlog["taskEndSendResultViaRPCTime"])
		driver_get_result_via_rpc_ts = float(jlog["DirverGetResultViaRPCTime"])
		exe_delta[executor_id].update_min(task_send_result_via_rpc_ts - driver_get_result_via_rpc_ts)
		# if executor_id == 1:
		# 	print "min", (task_send_result_via_rpc_ts - driver_get_result_via_rpc_ts)

print "#executorId xmin xmax (executorTime - driver_time = x)"
for exe_id in range(1, executor_num + 1):
	print exe_id, exe_delta[exe_id].min_x, exe_delta[exe_id].max_x
