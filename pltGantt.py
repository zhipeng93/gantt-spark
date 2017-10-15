import matplotlib
import matplotlib.pyplot as plt
import sys
from matplotlib import colors as mcolors
import matplotlib.patches as patches

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

font = {'family' : 'DejaVu Sans',
        'weight' : 'bold',
        'size'   : 22}

matplotlib.rc('font', **font)

def get_yticks(executor_num, core_num):
	l = [None] * (executor_num * core_num + 1)
	l[0] = "Driver"
	for i in range(1, executor_num * core_num + 1):
		executor_id = (i - 1) / core_num + 1
		core_id = i % core_num
		if core_id == 0:
			core_id += core_num
		l[i] = "E-" + str(executor_id) + "-C-" + str(core_id)
		# print i, l[i]
	return l

def getColor(activityType):
	if activityType in activity2ColorId:
		pass
	else:
		activity2ColorId[activityType] = len(activity2ColorId)

	return colors[activity2ColorId[activityType]]


def drawGantt(num_executor, num_core, log_file):
	# fig_title = "Gantt Chart for SGD in MLlib"
	fig = plt.figure(figsize=(23, num_core * num_executor + 5))
	# fig = plt.figure()
	y_ticks = get_yticks(num_executor, num_core)
	
	frame = fig.add_subplot(111)
	frame.set_xlabel("Figure: Time (seconds). \n Red vertical line stands for stage submission.\n"+ 
	"Green vertical line stands for stage completion.\n" + 
		"Grey vertical line stands for control message.")
	# frame.set_xlabel("Time (s)")
	frame.set_ylabel("Executor-ID-core-ID")
	# plt.suptitle(fig_title)
	frame.grid()
	frame.axis([-.5, 20, -0.5, num_executor * num_core + 0.5])

	frame.set_yticks(xrange(num_core * num_executor + 1), minor=False)
	frame.set_yticklabels(y_ticks)

	init_time = -1
	for line in open(log_file):
		if "minSubmission" in line:
			xx = float(line.strip().split(":")[1])
			if init_time == -1:
				init_time = xx
				frame.axvline((xx - init_time) / 1000000.0 / 1000.0, color='red', linestyle='--')
			elif init_time > xx:
				frame.axvline((xx - init_time) / 1000000.0 / 1000.0, color='red', linestyle='--')
				init_time = xx
			else:
				frame.axvline((xx - init_time) / 1000000.0 / 1000.0, color='red', linestyle='--')
				pass
		elif "minCompletion" in line:
			xx = float(line.strip().split(":")[1])
			frame.axvline((xx - init_time) / 1000000.0 / 1000.0, color='green', linestyle='-.')
			pass

		elif "ControlMessage" in line:
			drawLine(init_time, line, frame)
			pass
		else:
			drawBar(init_time, line, frame)

	handles, labels = plt.gca().get_legend_handles_labels()
	i =1
	while i < len(labels):
		if labels[i] in labels[:i]:
			del(labels[i])
			del(handles[i])
		else:
			i +=1

	frame.legend(handles, labels, loc="upper center", ncol=1, frameon=False,
		bbox_to_anchor=(1.17, 1.0))
	# print init_time
	# plt.tight_layout()
	plt.savefig(log_file + ".pdf", bbox_inches='tight')
		

def drawLine(init_time, line, frame):
	# activityType:ControlMessage=sender:0=send_ts:1505248946991000000=receiver:24=recv_ts:1505248946993000000
	infos = line.strip().split("=")
	sender = int(infos[1].split(":")[1])
	send_ts = (float(infos[2].split(":")[1]) - init_time) / 1000000.0 / 1000.0 # ns -- ms -- s
	receiver = int(infos[3].split(":")[1])
	recv_ts = (float(infos[4].split(":")[1]) - init_time) / 1000000.0 / 1000.0 # ns -- ms -- s
	# draw a curve from (sender_ts, sender) -> (recv_ts, receiver)
	if sender == receiver:
		print line
	plt.arrow(send_ts, sender, recv_ts - send_ts, receiver - sender, width=0,
			head_width=.03,head_length=.045,length_includes_head=True,color="grey", linestyle='-.')

	return


def drawBar(init_time, line, frame):
	# activityType:DriverTime=start_ts:1505248947722000000=duration:520000000=workerId:0=stageId:598
	infos = line.strip().split("=")
	activityType = infos[0].split(":")[1]
	start_ts = (float(infos[1].split(":")[1]) - init_time ) / 1000000.0 / 1000.0 # ns -- ms
	duration = float(infos[2].split(":")[1]) / 1000000.0 / 1000.0
	executorId = int(infos[3].split(":")[1])
	stageId = int(infos[4].split(":")[1])

	p = patches.Rectangle(
		(start_ts, executorId - 0.3), duration, 0.6,
		facecolor=getColor(activityType),
		label=activityType
		)

	frame.add_patch(p)


if __name__ == '__main__':
	num_executors = int(sys.argv[1])
	num_cores = int(sys.argv[2])
	gantt_input = sys.argv[3]
	drawGantt(num_executors, num_cores, gantt_input)

		