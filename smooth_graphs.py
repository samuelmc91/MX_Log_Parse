import os
import matplotlib.pyplot as plt
import pandas
import pandas as pd
import numpy as np
import datetime

def smooth_graphs(output_dir):
    files = [f for f in os.listdir(output_dir) if f.split('.')[-1] == 'csv']
    count = 1
    for file in files:
        print('Starting file: ', file)
        try:
            df = pd.read_csv(file)
        except Exception:
            print('\tFile {} failed\n'.format(file))
            continue
        if int(len(df.index)) <= 50:
            print('\tNot enough rows # of rows: ', str(len(df.index)), '\n')
            continue
        print('\t# of rows: ', str(len(df.index)), '\n')
        x = df['Time'].values
        y = df[df.columns[3]].values
        if count % 4 == 0:
            count = 1
        else:
            count += 1
        window_size = int(len(df.index) * .33)
        y_rolling = df[df.columns[3]].rolling(window_size).mean()[window_size:]
        x_rolling = df['Time'].values[window_size:]
        
        n_rolling_ticks = len(x_rolling) // 10
        n_ticks = len(df.index) // 10
        
        fig, axs = plt.subplots(3)
        fig.suptitle(file)
        axs[0].plot(x, y)
        axs[0].set_title('Original Graph')
        axs[0].set_xticks(axs[0].get_xticks()[::n_ticks])
        axs[0].tick_params(axis='x', rotation=-45)

        axs[1].plot(x_rolling, y_rolling)
        axs[1].set_title('Window Size: ' + str(window_size))
        axs[1].set_xticks(axs[1].get_xticks()[::n_rolling_ticks])
        axs[1].tick_params(axis='x', rotation=-45)
        
        dy = max(y_rolling) - min(y_rolling)
        x_der = y_rolling**2 + 1
        dxdy = np.gradient(x_der, dy)

        axs[2].plot(x_rolling, dxdy)
        axs[2].set_title('Derivative')
        axs[2].set_xticks(axs[2].get_xticks()[::n_rolling_ticks])
        axs[2].tick_params(axis='x', rotation=-45)

        fig.tight_layout(pad=2.0)
        fig.savefig(os.path.join(output_dir, file.split('.')[0] + '.png'))
        plt.close('all')
