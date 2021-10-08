import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import csv
import numpy as np

df = pd.read_csv(sys.argv[1])
# Cycle
x = df[df.columns[0]].values
# 
y = df[df.columns[2]].values
# Reliability 
z = df[df.columns[1]].values

fig, ax = plt.subplots()

############## Bar One Information ##############
# I set the original x tick values to integers to space them out which would not
# work using their cycle information. So here the graph x value is set to a list
# containing floats from 1 - the with of the bar to the length of x + 1.
# [0.7, 1.7, ...]
p1 = ax.bar([i - 0.3 for i in range(1,len(x) + 1)], y, color='tab:green', width=0.3,
            label='samples mounted', align='center')  # Also to note that both bars were changed back to center aligned 
# ax.bar_label(p1, label_type='edge', size=8)  # Changed bar label size to 8
ax.set_ylim([min(y)-0.1, max(y)+0.1])
# Here I used a numpy array to set the number of x ticks back to the correct amount.
# Without this, not every bar is labeled and changing the ticks back to the 
# cycle titles was not working
ax.set_xticks(np.arange(1, len(x)+1))
# Setting the x tick lables back to the cycle dates
ax.set_xticklabels(x)

############## Bar Two Information ##############
ax2 = ax.twinx()
p2 = ax2.bar([i + 0.1 for i in range(1,len(x) + 1)], z, color='m', width=0.3,
             label='samples mounted', align='center')
# ax2.bar_label(p2, label_type='edge', size=8)
ax2.set_ylim([min(z)-5, max(z)+2])

fig.savefig('test.png')
