def getColor(activityType):
	if activityType in activity2ColorId:
		pass
	else:
		activity2ColorId[activityType] = len(activity2ColorId)

	return colors[activity2ColorId[activityType]]


def categoryName(workerId, coreNum=1):
	if workerId == 0:
		return "Driver"
	else:
		executorId = (workerId + core_num - 1) / core_num
		coreId = (workerId + core_num - 1) - executorId * core_num
		return 'E{}C{}'.format(executorId, coreId) 


class Executor:
	def __init__(self, category):
		self.category = category #machine name, like E1C1
		self.segments = list() # list of task inforamtion

	def addTask(self, task):
		self.segments.append(task)

	def toString(self):
		print '{{\n\t"category": "{}",'.format(categoryName(self.category))
		print '\t"segments": ['
		for task in self.segments:
			task.toString()
		print '\t]'
		print '},'

class Task:
	def __init__(self, start_time, duration, task_color, task_name):
		self.start_time = start_time
		self.duration = duration
		self.task_color = task_color
		self.task_name = task_name

	def toString(self):
		# {"start": "1", "duration": "2", "color": "3", "task": "4"},
		print '\t\t{{"start": {}, "duration": {}, "color": "{}", "task": "{}"}},'\
		.format(self.start_time, self.duration, self.task_color, self.task_name)


import sys
from matplotlib import colors as mcolors

colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS).values()
activity2ColorId = dict()
activity2ColorId["Deserialization"] = 1
activity2ColorId["Computing"] = 2
activity2ColorId["ShuffleWrite"] = 3
activity2ColorId["Serialization"] = 4
activity2ColorId["PuttingIntoBlockManager"] = 5
activity2ColorId["DecodeDesc"] = 6
activity2ColorId["GCTime"] = 7
activity2ColorId["Broadcast"] = 8
activity2ColorId["DestoryBroadcast"] = 13
activity2ColorId["UpdateWeight"] = 9
activity2ColorId["JudgeConverge"] = 10
activity2ColorId["GettingResult"] = 11
activity2ColorId["ShuffleRead"] = 12


if __name__ == "__main__":
# TTs = [DriverOrTask() for i in range(MAX_TASK_NUM)]
	executor_num = 8
	core_num = 1
	worker_num = executor_num * core_num + 1 # plus driver
	executors = [Executor(i) for i in range(worker_num)]

	min_start = -1
	for line in open(sys.argv[1]):
		if "minSubmission" in line:
			ini_start = float(line.strip().split(":")[1])
			if min_start == -1:
				min_start = ini_start
			min_start = min(min_start, ini_start)

		if "activityType" in line:
			if "ControlMessage" in line:
				pass
			else:
				#activityType:PuttingIntoBlockManager=start_ts:1.5115150635e+18=
				#duration:0.0=workerId:1=stageId:7
				infos = line.strip().split("=")
				task_name = infos[0].split(":")[1]
				start_time = (float(infos[1].split(":")[1]) - min_start) / 1000000.0 
				duration = (float(infos[2].split(":")[1])) / 1000000.0 
				color = getColor(task_name)
				workerId = int(infos[3].split(":")[1])
				if duration < 1e-5:
					pass
				elif start_time > 180000:
					pass
				else:
					# def __init__(self, start_time, duration, task_color, task_name):
					executors[workerId].addTask(Task(start_time, duration, color, task_name))

	for i in range(worker_num):
		executors[worker_num - i - 1].toString()


