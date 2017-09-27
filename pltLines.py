import matplotlib
import matplotlib.pyplot as plt
import sys


attr_access = lambda i, y: [x[i] for x in y]
FONTSIZE = 20
optAvazuLoss = 0.34
delta = 0.05 # 0.001
tolerance = optAvazuLoss + delta
tolerance_str = "tolerance_" + str(delta)
NUM_TUPLE_SMALL=4041784

def formatNumber(num):
	# convert 4041784 --> 4M
	# 404178 --> 400k
	# 40417 --> 40k
	# 4041 --> 4k
	# 404 --> 400
	# 40 --> 40
	# 4 --> 4
	num = int(num)
	if num / 1000000 > 0:
		return str(num / 1000000) + "M"
	elif num / 100000 > 0:
		return str(num / 100000) + "00K"
	elif num / 10000 > 0:
		return str(num / 10000) + "0K"
	elif num / 1000 > 0:
		return str(num / 1000) + "K"
	elif num / 100 > 0:
		return str(num /  100) + "00"
	elif num / 10 > 0:
		return str(num / 10) + "0"
	else:
		return str(num)

def getLabelByFileName(inf):
	# file name is: gd-0.000001-5-6-1-1
	infos = inf.strip().split("-")
	infos[1] = str(formatNumber(float(infos[1]) * NUM_TUPLE_SMALL))
	return "-".join(infos)

def read_tuples(inf):
	# ghandTrainLoss=IterationId:50=EpochID:50.0=startLossTime:1505933213394=
	# EndLossTime:1505933213535=trainLoss:0.33544527915028527
	
	iteration_Ids = []
	times = []
	losses = []
	numTuples = []
	last_end_ts = -1
	for line in open(inf):
		infos = line.strip().split("=")
		iteration_Ids.append(int(infos[1].split(":")[1]))
		numTuples.append(int(float(infos[2].split(":")[1]) * 4041784))
		losses.append(float(infos[5].split(":")[1]))

		_start_ts = float(infos[3].split(":")[1])
		_end_ts = float(infos[4].split(":")[1])
		if last_end_ts == -1:
			times.append(1) # this is the first iteration
			last_end_ts = _end_ts
		else:
			times.append((times[-1] + _start_ts - last_end_ts) / 1000.0) #
			# last iteration complete time plus this iteration duration

	return times, iteration_Ids, losses, numTuples


def draw_curves(outf, infs): 
	# input some files with multi variables changes, each curve is good and not-shaking 
	# the legend is the name of input file

	fig_title = "Legend Format: minibatchSize-workerNum-coreNumber-stepSize-partitionPerCore"

	
	file_num = len(infs)
	_time = [None] * file_num
	_iter_id = [None] * file_num
	_loss = [None] * file_num
	_numTuples = [None] * file_num
	# labels = map(lambda x: x.strip().split("/")[-1].split(".")[0], infs)
	labels = []

	line_count = 0
	for inf in infs:
		_time[line_count], _iter_id[line_count], _loss[line_count], _numTuples[line_count] = read_tuples(inf)
		labels.append(getLabelByFileName(inf))
		line_count += 1

	fig = plt.figure(figsize=(23, 8))
	
	frame1 = fig.add_subplot(141)
	frame2 = fig.add_subplot(142)
	frame3 = fig.add_subplot(143)
	frame4 = fig.add_subplot(144)
	
	frame1.set_xlabel("time(s)", fontsize=FONTSIZE)
	frame1.set_ylabel("loss", fontsize=FONTSIZE)
	max_x = 0
	for i in xrange(file_num):
		frame1.plot(_time[i], _loss[i], label = labels[i])
		max_x = max(max_x, max(_time[i]))
		print "maxTime for file {} is {}".format(infs[i], max(_time[i]))
		
	max_x = int(max_x) + 1
	frame1.axhline(y=tolerance, linestyle='--')
	frame1.text(max_x * 0.4, tolerance + 0.0002, tolerance_str, fontsize = 15)
	frame1.legend(fontsize=FONTSIZE)
	frame1.set_xscale("log")
	
	frame2.set_xlabel("iteration(Id)", fontsize=FONTSIZE)
	frame2.set_ylabel("loss", fontsize=FONTSIZE)
	max_x = 0 # used for drawing tolerance
	for i in xrange(file_num):
		frame2.plot(_iter_id[i], _loss[i], label = labels[i])
		max_x = max(max_x, max(_iter_id[i]))
		
	max_x = int(max_x) + 1
	frame2.axhline(y=tolerance, linestyle='--')
	frame2.text(max_x * 0.4, tolerance + 0.0002, tolerance_str, fontsize = 15)
	frame2.legend(fontsize=FONTSIZE)
	frame2.set_xscale("log")

	frame3.set_xlabel("numTuples", fontsize=FONTSIZE)
	frame3.set_ylabel("loss", fontsize=FONTSIZE)
	max_x = 0
	for i in xrange(file_num):
		frame3.plot(_numTuples[i], _loss[i], label = labels[i])
		max_x = max(max_x, max(_numTuples[i]))
	
	max_x = int(max_x) + 1
	# frame3.plot(xrange(max_x), [tolerance] * max_x, '--')
	frame3.axhline(y=tolerance, linestyle='--')
	frame3.text(max_x * 0.4, tolerance + 0.0002, tolerance_str, fontsize = 15)
	frame3.legend(fontsize=FONTSIZE)
	frame3.set_xscale("log")

	frame4.set_xlabel("numTuples", fontsize=FONTSIZE)
	frame4.set_ylabel("time", fontsize=FONTSIZE)
	max_x = 0
	for i in xrange(file_num):
		frame4.plot(_numTuples[i], _time[i], label = labels[i])
		max_x = max(max_x, max(_time[i]))
		
	max_x = int(max_x) + 1

	frame4.legend(fontsize=FONTSIZE)
	frame4.set_xscale("log")
	frame4.set_yscale("log")

	plt.suptitle(fig_title, fontsize=FONTSIZE)
	plt.savefig(outf + "_" + tolerance_str + ".pdf")


if __name__ == '__main__':
	if len(sys.argv) < 3:
		print "at least one parameters"
		print "usage: python _.py outfile_name logfiles"
		sys.exit(1)
	else:
		draw_curves(sys.argv[1], sys.argv[2:])

		
