import sys
import json

MAX_JOB_NUM = 5000
MAX_TASK_NUM = MAX_JOB_NUM * 127
MAX_STAGE_NUM = MAX_JOB_NUM * 3

class DriverOrTask:
	# d = {} # this is the shared variable. All objects
	# from this class will share this variable.

	def __init__(self):
		# self refers to the newly created object
		self.d = {}
		pass

	def setDefaultAttr(self, names, value):
		for name in names:
			self.setAttrByName(name, value)

	def setAttrByName(self, name, value):
		self.d[name] = value

	def getAttrByName(self, name):
		return self.d[name]

	def printToConsole(self):
		for k in self.d:
			print k, self.d[k],
		print

	def containsKey(self, name):
		if name in self.d:
			return True
		else:
			return False

	def setTransformationTime(self, transformation_name, elementTime):
		start_key = transformation_name + "Start"
		end_key = transformation_name + "End"
		if self.getAttrByName(start_key) == 0 or elementTime < self.getAttrByName(start_key):
			self.setAttrByName(start_key, elementTime)
		
		if self.getAttrByName(end_key) == 0 or elementTime > self.getAttrByName(end_key):
			self.setAttrByName(end_key, elementTime)


def combine(slogs, commonlogs, transformationlogs, outfile_name):
	outf = open(outfile_name, "w")
	jsonLogs = []
	for line in open(slogs):
		jsonLogs.append(json.loads(line))
		# print xx['Event']
		# outf.write(json.dumps(xx) + "\n")

	driverInfos = processDriverInfo(commonlogs)
	mergeDriverInfo(jsonLogs, driverInfos)

	taskCommonInfos = processTaskInfo(commonlogs)
	mergeTaskInfo(jsonLogs, taskCommonInfos)

	transInfos = processTransformationInfo(transformationlogs)
	mergeTaskInfo(jsonLogs, transInfos)

	for jlog in jsonLogs:
		outf.write(json.dumps(jlog) + "\n")


def handleTransformationLine(line):
	line = line.strip().split(" ")[-1]
	infos = line.split("=")
	transformation_name = infos[0][8:]

	taskId = int(infos[1].split(":")[1])
	elementId = infos[3].split(":")[1]
	elementTime = infos[4].split(":")[1]
	return taskId, transformation_name, elementId, elementTime 


def processTransformationInfo(transformationlogs):
	TTs = [DriverOrTask() for i in range(MAX_TASK_NUM)]
	# we complete all the stage infos in each task, for the completion in spark-parser.
	transformationNames = ["logInputMapStart", "logInputMapEnd",
	"logSampleFilterStart", "logSampleFilterEnd", "logSeqOpStart",
	"logSeqOpEnd", "logMapPartitionWithIndexStart", "logMapPartitionWithIndexEnd",
	"logCombOpStart", "logCombOpEnd", "logGetValuesRDDStart", "logGetValuesRDDEnd"]
	for tt in TTs:
		tt.setDefaultAttr(transformationNames, 0)

	for line in open(transformationlogs):
		taskId, transformation_name, elementId, elementTime = handleTransformationLine(line)
		TTs[taskId].setTransformationTime(transformation_name, elementTime)
		# TTs[taskId].setAttrByName("taskId", taskId)
	return TTs


def mergeTaskInfo(jsonLogs, taskInfos):
	# merge taskInfo into jsonLogs, only merged into taskEndEvents
	for jlog in jsonLogs:
		if jlog["Event"] == "SparkListenerTaskEnd":
			taskId = int(jlog["Task Info"]["Task ID"])
			tmp_task = taskInfos[taskId]
			for k in tmp_task.d:
				jlog[k] = tmp_task.getAttrByName(k)


def mergeDriverInfo(jsonLogs, driverInfos):
	# merge driverInfos into jsonLogs, although the iterationId is not important, we still 
	# merge the driver into into the corresponding stage. 
	# That is: (driverInfo_id) --> stage(driverInfo_id * 3 + 8)
	# Note: here driverInfo[0] represents iteration one.
	for jlog in jsonLogs:
		if jlog["Event"] == "SparkListenerStageCompleted":
			stageId = int(jlog["Stage Info"]["Stage ID"])
			_driverInfo = driverInfos[stageId]
			for k in _driverInfo.d:
				jlog[k] = _driverInfo.getAttrByName(k)


def processDriverInfo(glogs):
	# driver information are ordered by iteration, so we do not add additional IDs for different iteration.
	# just use the IDs of lists
	IterationNum = MAX_STAGE_NUM
	DriverInfo = [DriverOrTask() for i in range(IterationNum)]
	# _driverInfo = DriverOrTask()
	driverAttr = ["BroadcastStartsTime", "BroadcastEndsTime",
	"DestroyBroadcastStartsTime", "DestroyBroadcastEndsTime",
	"updateWeightOnDriverStartsTime", "updateWeightOnDriverEndsTime",
	 "JudgeConvergeStartsTime", "JudgeConvergeEndsTime"]
	# logFormat: xxxxxx ghandCP=IterationId:1=BroadcastStartsTime:1505037507302
	for driver in DriverInfo:
		driver.setDefaultAttr(driverAttr, 0)
	for line in open(glogs):
		for attr in driverAttr:
			if attr in line:
				infos = line.strip().split("=")
				iterationId = int(infos[1].strip().split(":")[1]) # starts from 1, like 1, 2, 3...
				name, value = infos[2].strip().split(":")
				stageId = iterationId * 3 + 5 # e.g., 200-iteration is for stage 605.
				DriverInfo[stageId].setAttrByName(name, value)

	return DriverInfo

def processTaskInfo(glogs):
	TaskNum = MAX_TASK_NUM # start from zero.
	TaskInfos = [DriverOrTask() for i in range(TaskNum)]
	# all tasks have these metric, we do not set the default values.
	for line in open(glogs):
		if "ghandCP=EndsDecodeTaskDescription" in line:
			# log from CoarseGrainedExecutorEnd
			infos = line.strip().split("=")
			taskId = int(infos[2].strip().split(":")[1])
			for info in infos[3:]:
				name, value = info.strip().split(":")
				TaskInfos[taskId].setAttrByName(name, value)
		if "ghandCP=Executor=" in line:
			# log from Executor.scala
			infos = line.strip().split("=")
			taskId = int(infos[5].strip().split(":")[1])
			for info in infos[3:]:
				name, value = info.strip().split(":")
				TaskInfos[taskId].setAttrByName(name, value)

		if "ExecutorDeserialStarts" in line:
			# log from ShuffleMapTask and ResultTask
			infos = line.strip().split("=")
			taskId = int(infos[1].split(":")[1])
			for info in infos[2:]:
				name, value = info.strip().split(":")
				TaskInfos[taskId].setAttrByName(name, value)
		if "driverLanuchTaskViaRPC" in line:
			# lanuch time from driver
			infos = line.strip().split("=")
			taskId = int(infos[2].split(":")[1])
			name, value = infos[3].split(":")
			TaskInfos[taskId].setAttrByName(name, value)

	return TaskInfos


if __name__ == "__main__":
	app_dir = sys.argv[1]
	spark_logs = app_dir + "/spark_part_log"
	common_logs = app_dir + "/task_common_log"
	transformation_logs = app_dir + "/task_transformation_log"
	combine(spark_logs, common_logs, transformation_logs, "resources/resultLogs/resultLogs")
