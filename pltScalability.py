# import matplotlib
# import matplotlib.pyplot as plt
# import sys
# # run for different number of machines
# fig = plt.figure()
# frame = fig.add_subplot(111)


# plt.plot(x, y_ratio_upbound)
# # plt.plot(x, y_compute, label="computation time")
# plt.ylabel('upper bound for speedup')
# plt.xlabel('model size')
# plt.set_xticks(x_label)
# # plt.set
# frame.set_xticks(x_label, minor=False)

# plt.show()

# plt.savefig("scalability.pdf")

import matplotlib.pyplot as plt

# y_communicate = [1.1, 4.58, 9.18]
# y_compute = [1.67, 2.59, 3.96]
# y_ratio_upbound = [None] * 3
# for i in range(0, 3):
# 	y_ratio_upbound[i] = (y_compute[i] + y_communicate[i]) / y_compute[i] * 100
# # x_label = ["1M", "3.2M", "8.3M"]
# x_label = ["CTR", "URL", "webspam"]

# x = [1, 2, 3]

# fig = plt.figure(figsize=(15, 8))
# frame1 = fig.add_subplot(121)
# frame2 = fig.add_subplot(122)
# frame1.set_ylabel("upper bound of speedup/percent", fontsize=20)
# frame1.set_xlabel("dataset", fontsize=20)
# frame1.plot(x, y_ratio_upbound)
# frame1.set_xticks(x, minor=False)
# frame1.set_xticklabels(x_label, fontsize=20)

# frame2.set_ylabel("time for communication(s)/epoch", fontsize=20)
# frame2.set_xlabel("dataset", fontsize=20)
# frame2.plot(x, y_communicate)
# frame2.set_xticks(x, minor=False)
# frame2.set_xticklabels(x_label, fontsize=20)
# plt.savefig("xxxx.pdf")
y_driver = [2.5, 8.6, 16.5]
y_shuffle = [1.1, 4.1, 2.8]
x = [1, 2, 3]
x_label = ["CTR", "URL", "webspam"]
fig = plt.figure(figsize=(15, 8))
frame1 = fig.add_subplot(111)
frame1.set_ylabel("time per epoch(second)", fontsize=20)
frame1.set_xlabel("dataset", fontsize=20)
frame1.plot(x, y_driver, label="model average + driver collect")
frame1.plot(x, y_shuffle, label="model average + shuffle")
frame1.set_xticks(x, minor=False)
frame1.set_xticklabels(x_label, fontsize=20)
plt.savefig("xxxx.pdf")
