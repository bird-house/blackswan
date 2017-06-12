
from os import listdir
from os import path

from ocgis import RequestDataset, OcgOperations
from ocgis.contrib import library_icclim as lic

from datetime import datetime as dt

from flyingpigeon.ocgis_module import call
from flyingpigeon.utils import get_values, get_time
from numpy import ma
import uuid

p = '/home/nils/birdhouse/var/lib/pywps/cache/malleefowl/esgf1.dkrz.de/thredds/fileServer/cordex/cordex/output/AFR-44/\
     MPI-CSC/MPI-M-MPI-ESM-LR/historical/r1i1p1/MPI-CSC-REMO2009/v1/day/tas/v20160412/'

resource = [path.join(p, nc) for nc in listdir(p)]
resource.sort()
# rd = RequestDataset(ncs[0])
indice = 'TG'
percentile = 90
var = 'tas'
window_width = 5

dt1 = dt(1970, 01, 01)
dt2 = dt(2000, 12, 31)
period = [dt1, dt2]  # we will calculate the indice for 10 years
#

rd = RequestDataset(recource, 'tas', time_range=period)

from ocgis.constants import DimensionMapKey
rd.dimension_map.set_bounds(DimensionMapKey.TIME, None)


kwds = {'percentile': 90, 'window_width': 5}
calc = [{'func': 'daily_perc', 'name': 'dp', 'kwds': kwds}]
ops = ocgis.OcgOperations(dataset=rd, geom='state_boundaries', select_ugid=[23], calc=calc,
                          output_format='nc', time_region={'year': [1980, 1990]}).execute()
print ops
