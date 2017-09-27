import matplotlib
import matplotlib.pyplot as plt
import sys
# run for different number of machines
gd_times1 = [66.702, 60.154, 61.148, 62.973, 67.438]
gd_times2 = [67.609, 65.854, 63.694, 68.486, 68.143]
gd_times3 = [62.488, 63.78, 63.977, 65.624, 68.923]

# time for mini-batch 0.001
mgd_times1 = [55.702, 55.37, 56.723, 64.528, 65.869]
mgd_times2 = [56.592, 58.019, 56.544, 64.235, 66.239]
mgd_times3 = [56.15, 55.075, 57.177, 60.766, 64.324]

gd_time_total = [None] * 5
mgd_time_total = [None] * 5
for i in range(5):
	gd_time_total[i] = (gd_times1[i] + gd_times2[i] + gd_times3[i]) / 3
	mgd_time_total[i] = (mgd_times1[i] + mgd_times2[i] + mgd_times3[i]) / 3
y_gd = [i / 50 for i in gd_time_total]
y_mgd = [i / 50 for i in mgd_time_total]
x = [1, 2, 3, 4, 5]
plt.plot(x, y_gd, label="batchsize:4M")
plt.plot(x, y_mgd, label="batchsize:4K")
plt.legend()
plt.ylabel('time per batch(s)')
plt.xlabel('number of machine')
plt.savefig("scalability.pdf")