import numpy as np
import matplotlib.pyplot as plt

import matplotlib.patches as mpatches
from decimal import Decimal


def plot_map(params, data):
    n, bins, patches = plt.hist(data, 50, facecolor='green', alpha=0.75)

    plt.xlabel('data')
    plt.ylabel('Count')
    plt.title(r'$\mathrm{Histogram\ of\ data}$')
    # plt.yscale('log', nonposy='clip')

    # show rms, median and mean
    rms = np.power((sum(np.power(data, 2))) / len(data), 0.5)
    median = np.median(data)
    mean = np.mean(data)

    mean_patch = mpatches.Patch(color='red',
                                label='mean %.5E' % Decimal(str(mean)))
    median_patch = mpatches.Patch(color='green',
                                  label='median %.5E' % Decimal(str(median)))
    rms_patch = mpatches.Patch(color='blue',
                               label='rms %.5E' % Decimal(str(rms)))

    plt.legend(handles=[mean_patch, median_patch, rms_patch])

    plt.axvline(mean, color='r', linestyle='dashed', linewidth=2)
    plt.axvline(median, color='g', linestyle='dashed', linewidth=2)
    plt.axvline(rms, color='b', linestyle='dashed', linewidth=2)

    plt.grid(True)

    plt.savefig(params['name'] + '.png')
    plt.clf()
