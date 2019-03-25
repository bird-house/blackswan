import numpy as np

from os import environ

from scipy.spatial.distance import cdist
from scipy.stats.mstats import mquantiles

from blackswan.utils import get_values, get_index_lat, get_index_lon, get_variable

import ctypes
from multiprocessing import Pool

def _calc_dist(sp_vector):
    _distance = cdist(sp_vector, glob_dat, metric=glob_distance) 
    _logdista = -np.log(_distance)
    _x = _logdista[~np.isinf(_logdista)]
    _logdista = _x[~np.isnan(_x)]
    _thresh = mquantiles(_logdista, glob_quanti, alphap=glob_abal, betap=glob_abal)
    # NOT ORIGINAL
    _logdista = -np.log(_distance)[0]
    
    _Li = [i for i in range(len(_logdista)) if (_logdista[i] > _thresh)and(_logdista[i]<0)]
    _Ti = np.diff(_Li)
    _N = len(_Ti)
    _q = 1.-glob_quanti
    _Si = _Ti-1
    _Ncc = [i for i in range(len(_Si)) if (_Si[i] > 0)]
    _Nc = len(_Ncc)

    _theta = (sum(_q*_Si)+_N+_Nc-np.sqrt(((sum(_q*_Si)+_N+_Nc)**2)-8*_Nc*sum(_q*_Si)))/(2*sum(_q*_Si))
    _logdista = np.sort(_logdista)

    _findidx = [i for i in range(len(_logdista)) if (_logdista[i] > _thresh)and(_logdista[i]<0)]

    _logextr = _logdista[_findidx[0]:len(_logdista)-1]
    _dim = 1/np.mean(_logextr-_thresh)

    return _dim, _theta


def localdims_par(resource, ap=0.5, variable=None, distance='euclidean'):
    """
    calculating of a local dimentions and persistence

    :param resource: str or list of str containing the netCDF file path

    :return 2 arrays: local dimentions and persistence
    """

    # ===================================================
    # only for linux
    try:
        mkl_rt = ctypes.CDLL('libmkl_rt.so')
        nth = mkl_rt.mkl_get_max_threads()
        mkl_rt.mkl_set_num_threads(ctypes.byref(ctypes.c_int(64)))
        nth = mkl_rt.mkl_get_max_threads()
        environ['MKL_NUM_THREADS'] = str(nth)
        environ['OMP_NUM_THREADS'] = str(nth)
    except:
        pass
    # ================================================================

    if variable is None:
        variable = get_variable(resource)

    data = get_values(resource, variable=variable)

    lat_index = get_index_lat(resource, variable=variable)
    lon_index = get_index_lon(resource, variable=variable)

    # TODO: should be 3D with TIME first.
    # Think how to operate with 4D and unknown stucture (lat,lon,level,time) for expample.

    # dat=data.reshape((data.shape[0],data.shape[1]*data.shape[2]))
    global glob_dat
    glob_dat=data.reshape((data.shape[0],data.shape[lat_index]*data.shape[lon_index]))

    # Quantile definition
    global glob_quanti
    glob_quanti=0.98

    # npoints=np.size(dat,0)
    # npoints=np.size(data,0)
    npoints=len(data[:,0])

    dim=np.empty((npoints))
    dim.fill(np.nan)
    theta=np.empty((npoints))
    theta.fill(np.nan)

    global glob_abal
    glob_abal=ap
    global glob_distance
    glob_distance=distance

    # global dat as list to be used by pool.map
    l_dat = [glob_dat[i,:].reshape(1,-1) for i in range(npoints)]

    # multi CPU 
    pool = Pool()
    res_p=pool.map(_calc_dist, l_dat)
    pool.close()
    pool.join()

    # collect results
    for i,j in enumerate(res_p): 
        dim[i]=res_p[i][0]
        theta[i]=res_p[i][1]

    return dim, theta


def localdims(resource, ap=0.5, variable=None, distance='euclidean'):
    """
    calculating of a local dimentions and persistence

    :param resource: str or list of str containing the netCDF file path

    :return 2 arrays: local dimentions and persistence
    """

    if variable is None:
        variable = get_variable(resource)

    data = get_values(resource, variable=variable)

    lat_index = get_index_lat(resource, variable=variable)
    lon_index = get_index_lon(resource, variable=variable)

    # TODO: should be 3D with TIME first.
    # Think how to operate with 4D and unknown stucture (lat,lon,level,time) for expample.

    # dat=data.reshape((data.shape[0],data.shape[1]*data.shape[2]))
    dat=data.reshape((data.shape[0],data.shape[lat_index]*data.shape[lon_index]))

    # Quantile definition
    quanti=0.98

    # npoints=np.size(dat,0)
    # npoints=np.size(data,0)
    npoints=len(data[:,0])

    dim=np.empty((npoints))
    dim.fill(np.nan)
    theta=np.empty((npoints))
    theta.fill(np.nan)

    l=0

    # Calculation of total distance matrix

    dist=cdist(dat, dat, metric=distance)
    # print np.shape(dist)

    # 0.5 = Python and Mathlab, 1 = R
    abal=ap
    # abal=0.5

    for l in range(npoints):

        distance=dist[:,l]

        logdista=-np.log(distance)
        x = logdista[~np.isinf(logdista)]
        logdista = x[~np.isnan(x)]
        thresh=mquantiles(logdista, quanti, alphap=abal, betap=abal)
        # NOT ORIGINAL
        logdista=-np.log(distance)
        Li = [i for i in range(len(logdista)) if (logdista[i] > thresh)and(logdista[i]<0)]
        Ti=np.diff(Li)
        N=len(Ti)
        q=1.-quanti
        Si=Ti-1
        Ncc=[i for i in range(len(Si)) if (Si[i] > 0)]
        Nc=len(Ncc)

        theta[l]=(sum(q*Si)+N+Nc-np.sqrt(((sum(q*Si)+N+Nc)**2)-8*Nc*sum(q*Si)))/(2*sum(q*Si))
        logdista=np.sort(logdista)

        findidx=[i for i in range(len(logdista)) if (logdista[i] > thresh)and(logdista[i]<0)]

        logextr=logdista[findidx[0]:len(logdista)-1]
        # logextr=logdista[findidx[[1]]:(length(logdista)-1)]

        dim[l]=1/np.mean(logextr-thresh)

    return dim, theta
