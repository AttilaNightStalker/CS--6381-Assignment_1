
from mpl_toolkits import mplot3d
import matplotlib.pyplot as plt
import numpy as np

def plotResult():
	with open("record.txt", "r") as fp:
		data = list(map(lambda x: x.split(","), filter(lambda x: not x == "", fp.read().split("\n"))))

	P = np.array([eval(d[0]) for d in data])
	S = np.array([eval(d[1]) for d in data])
	t = np.array([eval(d[2]) for d in data])

	print(P)
	print(S)
	print(t)

	fig = plt.figure()
	ax = plt.axes(projection='3d')
	ax.plot_trisurf(P, S, t, linewidth=0, antialiased=True)
	ax.set_xlabel('publisher number')
	ax.set_ylabel('subscriber number')
	ax.set_zlabel('average delay');
	plt.show()

plotResult()