from matplotlib import use
use('Agg')
# no X11 server ... must be run first

from matplotlib import pyplot as plt
from matplotlib.cm import get_cmap

plt.switch_backend('agg')

import cartopy.crs as ccrs
import numpy as np
from netCDF4 import Dataset
import pwd
import os
import uuid


def simple_plot(resource, variable='air', lat='lat', lon='lon', timestep=0, output=None):
    """
    Generates a nice and simple plot.
    """
    print("Plotting {}, timestep {} ...".format(resource, timestep))

    pl_data = Dataset(resource)

    pl_val = pl_data.variables[variable][timestep,:,:]
    pl_lat = pl_data.variables[lat][:]
    pl_lon = pl_data.variables[lon][:]

    fig = plt.figure()
    fig.set_size_inches(18.5, 10.5, forward=True)

    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines(linewidth=0.8)
    ax.gridlines()

    vmin = np.min(pl_val)
    vmax = np.max(pl_val)

    levels = np.linspace(vmin, vmax, 30)

    cmap = get_cmap("RdBu_r")

    data_map = ax.contourf(pl_lon, pl_lat, pl_val, levels=levels, extend='both', cmap=cmap, projection=ccrs.PlateCarree())
    data_cbar = plt.colorbar(data_map, extend='both', shrink=0.6)
    data_cont = ax.contour(pl_lon, pl_lat, pl_val, levels=levels, linewidths=0.5, colors="white", linestyles='dashed', projection=ccrs.PlateCarree())

    plt.clabel(data_cont, inline=1, fmt='%1.0f')
    title = 'Simple plot for %s' % (variable)
    plt.title(title)
    plt.tight_layout()

    if not output:
        output = 'myplot_%s.png' % (uuid.uuid1())

    plt.savefig(output)
    fig.clf()
    plt.close(fig)

    print("Plot written to {}".format(output))
    return output
