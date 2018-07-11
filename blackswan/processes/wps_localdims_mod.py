# import os
from os import path, environ
from datetime import datetime as dt
from datetime import time as dt_time
# from datetime import date
import time  # performance test

import subprocess
import uuid
# later goes to utils
from netCDF4 import Dataset

from numpy import savetxt, loadtxt, column_stack

from blackswan import analogs
from blackswan import config

from blackswan.ocgis_module import call
from blackswan.datafetch import get_level
from blackswan.utils import get_variable, get_time
from blackswan.utils import rename_complexinputs
from blackswan.utils import archiveextract
from blackswan.utils import get_timerange, get_calendar

#from blackswan.calculation import localdims
from blackswan.localdims import localdims, localdims_par

from blackswan.weatherregimes import _TIMEREGIONS_, _MONTHS_

from pywps import Process
from pywps import LiteralInput, LiteralOutput
from pywps import ComplexInput, ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata
from blackswan.log import init_process_logger

import logging
LOGGER = logging.getLogger("PYWPS")


class LocaldimsModProcess(Process):
    def __init__(self):
        inputs = [
            ComplexInput('resource', 'Resource',
                         abstract='NetCDF Files or archive (tar/zip) containing daily netCDF files.',
                         metadata=[Metadata('Info')],
                         min_occurs=1,
                         max_occurs=1000,
                         supported_formats=[
                             Format('application/x-netcdf'),
                             Format('application/x-tar'),
                             Format('application/zip'),
                         ]),

            LiteralInput('BBox', 'Bounding Box',
                         data_type='string',
                         abstract="Enter a bbox: min_lon, max_lon, min_lat, max_lat."
                            " min_lon=Western longitude,"
                            " max_lon=Eastern longitude,"
                            " min_lat=Southern or northern latitude,"
                            " max_lat=Northern or southern latitude."
                            " For example: -80,50,20,70",
                         min_occurs=0,
                         max_occurs=1,
                         default='-20,40,30,70',
                         ),

            LiteralInput("level", "Vertical level",
                         abstract="Vertical level for geopotential (hPa), only if zg variable used",
                         default='500',
                         data_type='integer',
                         min_occurs=0,
                         max_occurs=1,
                         allowed_values=[1000, 850, 700, 500, 250, 100, 50, 10]
                         ),

            LiteralInput("season", "Time region",
                         abstract="Select the months to define the time region (all == whole year will be analysed)",
                         default="DJF",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=['January','February','March','April','May','June','July','August','September','October','November','December'] + _TIMEREGIONS_.keys()
                         ),

            LiteralInput('dateSt', 'Start date of analysis period',
                         data_type='date',
                         abstract='First day of the period to be analysed',
                         default='2000-01-01',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('dateEn', 'End date of analysis period',
                         data_type='date',
                         abstract='Last day of the period to be analysed',
                         default='2002-12-31',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput("dist", "Distance",
                         abstract="Distance function to define analogues",
                         default='euclidean',
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         allowed_values=['euclidean', 'mahalanobis', 'cosine']
                         ),

            LiteralInput("method", "Method",
                         abstract="Method of calculation: Python(full dist matrix at once), R(full dist matrix at once), R_wrap(dist matrix row by row on multiCPUs)",
                         default='Python',
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         allowed_values=['Python', 'Python_wrap', 'R', 'R_wrap']
                         ),
        ]

        outputs = [

            ComplexOutput("ldist", "Distances File",
                          abstract="mulit-column text file",
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
                          ),
            ComplexOutput("ldist_seas", "Distances File for selected season (selection from all results)",
                          abstract="mulit-column text file",
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
                          ),
            ComplexOutput("ld_pdf", "Scatter plot dims/theta",
                          abstract="Scatter plot dims/theta",
                          supported_formats=[Format('image/pdf')],
                          as_reference=True,
                          ),
            ComplexOutput("ld2_pdf", "Scatter plot dims/theta",
                          abstract="Scatter plot dims/theta",
                          supported_formats=[Format('image/pdf')],
                          as_reference=True,
                          ),
            ComplexOutput("ld2_seas_pdf", "Scatter plot dims/theta for season",
                          abstract="Scatter plot dims/theta for season",
                          supported_formats=[Format('image/pdf')],
                          as_reference=True,
                          ),
            ComplexOutput('output_log', 'Logging information',
                          abstract="Collected logs during process run.",
                          as_reference=True,
                          supported_formats=[Format('text/plain')]
                          ),
        ]

        super(LocaldimsModProcess, self).__init__(
            self._handler,
            identifier="localdims_mod",
            title="Calculation of local dimentions and persistence (based on climate model data)",
            abstract='Calculation of local dimentions and persistence (based on climate model data)',
            version="0.10",
            metadata=[
                Metadata('LSCE', 'http://www.lsce.ipsl.fr/en/index.php'),
                Metadata('Doc', 'http://flyingpigeon.readthedocs.io/en/latest/'),
            ],
            inputs=inputs,
            outputs=outputs,
            status_supported=True,
            store_supported=True,
        )

    def _handler(self, request, response):
        init_process_logger('log.txt')
        response.outputs['output_log'].file = 'log.txt'

        LOGGER.info('Start process')
        response.update_status('execution started at : {}'.format(dt.now()), 5)

        process_start_time = time.time()  # measure process execution time ...
        start_time = time.time()  # measure init ...

        ################################
        # reading in the input arguments
        ################################

        # response.update_status('execution started at : %s ' % dt.now(), 5)
        # start_time = time.time()  # measure init ...

        ################################
        # reading in the input arguments
        ################################

        try:
            response.update_status('read input parameter : %s ' % dt.now(), 10)
            resource = archiveextract(resource=rename_complexinputs(request.inputs['resource']))
            dateSt = request.inputs['dateSt'][0].data
            dateEn = request.inputs['dateEn'][0].data

            bboxDef = '-20,40,30,70'  # in general format
            # level = 500

            season = request.inputs['season'][0].data

            level = request.inputs['level'][0].data
            if (level == 500):
                dummylevel = 1000  # dummy workaround for cdo sellevel
            else:
                dummylevel = 500
            LOGGER.debug('LEVEL selected: %s hPa' % (level))

            bbox = []
            bboxStr = request.inputs['BBox'][0].data
            LOGGER.debug('BBOX selected by user: %s ' % (bboxStr))
            bboxStr = bboxStr.split(',')

            # Checking for wrong cordinates and apply default if nesessary
            if (abs(float(bboxStr[0])) > 180 or
                    abs(float(bboxStr[1]) > 180) or
                    abs(float(bboxStr[2]) > 90) or
                    abs(float(bboxStr[3])) > 90):
                bboxStr = bboxDef  # request.inputs['BBox'].default  # .default doesn't work anymore!!!
                LOGGER.debug('BBOX is out of the range, using default instead: %s ' % (bboxStr))
                bboxStr = bboxStr.split(',')

            # for i in bboxStr: bbox.append(int(i))
            bbox.append(float(bboxStr[0]))
            bbox.append(float(bboxStr[2]))
            bbox.append(float(bboxStr[1]))
            bbox.append(float(bboxStr[3]))
            LOGGER.debug('BBOX for ocgis: %s ' % (bbox))
            LOGGER.debug('BBOX original: %s ' % (bboxStr))

            distance = request.inputs['dist'][0].data
            method = request.inputs['method'][0].data

            LOGGER.info('input parameters set')
            response.update_status('Read in and convert the arguments', 20)
        except Exception as e:
            msg = 'failed to read input prameter %s ' % e
            LOGGER.error(msg)
            raise Exception(msg)

        ######################################
        # convert types and set environment
        ######################################
        try:

            # not nesessary if fix ocgis_module.py
            dateSt = dt.combine(dateSt, dt_time(12, 0))
            dateEn = dt.combine(dateEn, dt_time(12, 0))

            # Check if 360_day calendar:
            try:
                if type(resource) is not list:
                    resource = [resource]

                modcal, calunits = get_calendar(resource[0])
                if '360_day' in modcal:
                    if dateSt.day == 31:
                        dateSt = dateSt.replace(day=30)
                        LOGGER.debug('Date has been changed for: %s' % (dateSt))
                    if dateEn.day == 31:
                        dateEn = dateEn.replace(day=30)
                        LOGGER.debug('Date has been changed for: %s' % (dateEn))
            except:
                LOGGER.debug('Could not detect calendar')

            start = dateSt
            end = dateEn
            time_range = [start, end]

            LOGGER.info('environment set')
        except Exception as e:
            msg = 'failed to set environment %s ' % e
            LOGGER.error(msg)
            raise Exception(msg)

        LOGGER.debug("init took %s seconds.", time.time() - start_time)
        response.update_status('Read in and convert the arguments', 30)

        ########################
        # input data preperation
        ########################

        # TODO: Check if files containing more than one dataset

        response.update_status('Start preparing input data', 40)
        start_time = time.time()  # mesure data preperation ...
        try:
            # TODO: Add selection of the level. maybe bellow in call(..., level_range=[...,...])

            if type(resource) == list:
                # resource.sort()
                resource = sorted(resource, key=lambda i: path.splitext(path.basename(i))[0])
            else:
                resource = [resource]

            # ===============================================================
            # REMOVE resources which are out of interest from the list
            # (years > and < than requested for calculation)

            tmp_resource = []

            for re in resource:
                s, e = get_timerange(re)
                tmpSt = dt.strptime(s, '%Y%m%d')
                tmpEn = dt.strptime(e, '%Y%m%d')
                if ((tmpSt <= end) and (tmpEn >= start)):
                    tmp_resource.append(re)
                    LOGGER.debug('Selected file: %s ' % (re))
            resource = tmp_resource

            # Try to fix memory issue... (ocgis call for files like 20-30 gb... )
            # IF 4D - select pressure level before domain cut
            #
            # resource properties
            ds = Dataset(resource[0])
            variable = get_variable(resource[0])
            var = ds.variables[variable]
            dims = list(var.dimensions)
            dimlen = len(dims)

            try:
                model_id = ds.getncattr('model_id')
            except AttributeError:
                model_id = 'Unknown_model'

            LOGGER.debug('MODEL: %s ' % (model_id))

            lev_units = 'hPa'

            if (dimlen > 3):
                lev = ds.variables[dims[1]]
                # actually index [1] need to be detected... assuming zg(time, plev, lat, lon)
                lev_units = lev.units

                if (lev_units == 'Pa'):
                    level = level * 100
                    dummylevel = dummylevel * 100
                    # TODO: OR check the NAME and units of vertical level and find 200 , 300, or 500 mbar in it
                    # Not just level = level * 100.

            # Get Levels
            from cdo import Cdo
            cdo = Cdo(env=environ)

            lev_res = []
            if(dimlen > 3):
                for res_fn in resource:
                    tmp_f = 'lev_' + path.basename(res_fn)
                    try:
                        tmp_f = call(resource=res_fn, variable=variable, spatial_wrapping='wrap',
                                     level_range=[int(level), int(level)], prefix=tmp_f[0:-3])
                    except:
                        comcdo = '%s,%s' % (level, dummylevel)
                        cdo.sellevel(comcdo, input=res_fn, output=tmp_f)
                    lev_res.append(tmp_f)
            else:
                lev_res = resource

            # ===============================================================
            # TODO: Before domain, Regrid to selected grid! (???) if no rean.
            # ================================================================

            # Get domain
            regr_res = []
            for res_fn in lev_res:
                tmp_f = 'dom_' + path.basename(res_fn)
                comcdo = '%s,%s,%s,%s' % (bbox[0], bbox[2], bbox[1], bbox[3])
                try:
                    tmp_f = call(resource=res_fn, geom=bbox, spatial_wrapping='wrap', prefix=tmp_f[0:-3])
                except:
                    cdo.sellonlatbox(comcdo, input=res_fn, output=tmp_f)
                regr_res.append(tmp_f)

            # ============================
            # Block to collect final data
            if (dimlen > 3):
                res_tmp_tmp = get_level(regr_res, level=level)
                variable = 'z%s' % level
                res_tmp = call(resource=res_tmp_tmp, variable=variable, time_range=time_range)
            else:
                res_tmp = call(resource=regr_res, time_range=time_range, spatial_wrapping='wrap')
            #######################################################################################

        except Exception as e:
            msg = 'failed to prepare archive and simulation files %s ' % e
            LOGGER.debug(msg)
            raise Exception(msg)
        LOGGER.debug("data preperation took %s seconds.", time.time() - start_time)

        # -----------------------
        # try:
        #     import ctypes
        #     # TODO: This lib is for linux
        #     mkl_rt = ctypes.CDLL('libmkl_rt.so')
        #     nth = mkl_rt.mkl_get_max_threads()
        #     LOGGER.debug('Current number of threads: %s' % (nth))
        #     mkl_rt.mkl_set_num_threads(ctypes.byref(ctypes.c_int(64)))
        #     nth = mkl_rt.mkl_get_max_threads()
        #     LOGGER.debug('NEW number of threads: %s' % (nth))
        #     # TODO: Does it \/\/\/ work with default shell=False in subprocess... (?)
        #     environ['MKL_NUM_THREADS'] = str(nth)
        #     environ['OMP_NUM_THREADS'] = str(nth)
        # except Exception as e:
        #     msg = 'Failed to set THREADS %s ' % e
        #     LOGGER.debug(msg)
        # -----------------------

        response.update_status('Start DIM calc', 50)

        # Calculation of Local Dimentsions ==================
        LOGGER.debug('Calculation of the distances using: %s metric' % (distance))
        LOGGER.debug('Calculation of the dims with: %s' % (method))

        dim_filename = '%s.txt' % model_id
        tmp_dim_fn = '%s.txt' % uuid.uuid1()
        Rsrc = config.Rsrc_dir()

        if (method == 'Python'):
            try:
                l_dist, l_theta = localdims(resource=res_tmp, variable=variable, distance=str(distance))
                response.update_status('**** Dims with Python suceeded', 60)
            except:
                LOGGER.exception('NO! output returned from Python call')

        if (method == 'Python_wrap'):
            try:
                l_dist, l_theta = localdims_par(resource=res_tmp, variable=variable, distance=str(distance))
                response.update_status('**** Dims with Python suceeded', 60)
            except:
                LOGGER.exception('NO! output returned from Python call')

        if (method == 'R'):
            # from os.path import join
            Rfile = 'localdimension_persistence_fullD.R'
            args = ['Rscript', path.join(Rsrc, Rfile),
                    '%s' % res_tmp, '%s' % variable,
                    '%s' % tmp_dim_fn]
            LOGGER.info('Rcall builded')
            LOGGER.debug('ARGS: %s' % (args))

            try:
                output, error = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
                LOGGER.info('R outlog info:\n %s ' % output)
                LOGGER.exception('R outlog errors:\n %s ' % error)
                if len(output) > 0:
                    response.update_status('**** Dims with R suceeded', 60)
                else:
                    LOGGER.exception('NO! output returned from R call')
                # HERE READ DATA FROM TEXT FILES
                R_resdim = loadtxt(fname=tmp_dim_fn, delimiter=',')
                l_theta = R_resdim[:, 0]
                l_dist = R_resdim[:, 1]
            except:
                msg = 'Dim with R'
                LOGGER.exception(msg)
                raise Exception(msg)

        if (method == 'R_wrap'):
            # from os.path import join
            Rfile = 'localdimension_persistence_serrD.R'
            args = ['Rscript', path.join(Rsrc, Rfile),
                    '%s' % res_tmp, '%s' % variable,
                    '%s' % tmp_dim_fn]
            LOGGER.info('Rcall builded')
            LOGGER.debug('ARGS: %s' % (args))

            try:
                output, error = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
                LOGGER.info('R outlog info:\n %s ' % output)
                LOGGER.exception('R outlog errors:\n %s ' % error)
                if len(output) > 0:
                    response.update_status('**** Dims with R_wrap suceeded', 60)
                else:
                    LOGGER.exception('NO! output returned from R call')
                # HERE READ DATA FROM TEXT FILES
                R_resdim = loadtxt(fname=tmp_dim_fn, delimiter=',')
                l_theta = R_resdim[:, 0]
                l_dist = R_resdim[:, 1]
            except:
                msg = 'Dim with R_wrap'
                LOGGER.exception(msg)
                raise Exception(msg)

        try:
            res_times = get_time(res_tmp)
        except:
            LOGGER.debug('Not standard calendar')
            res_times = analogs.get_time_nc(res_tmp)

        # plot 1
        ld_pdf = analogs.pdf_from_ld(x=l_dist, y=l_theta)
        #

        res_times=[res_times[i].isoformat().strip().split("T")[0].replace('-','') for i in range(len(res_times))]

        # concatenation of values
        concat_vals = column_stack([res_times, l_theta, l_dist])
        savetxt(dim_filename, concat_vals, fmt='%s', delimiter=',')

        # output season
        try:
            seas = _TIMEREGIONS_[season]['month'] # [12, 1, 2]
            LOGGER.info('Season to grep from TIMEREGIONS: %s ' % season)
            LOGGER.info('Season N to grep from TIMEREGIONS: %s ' % seas)
        except:
            LOGGER.info('No months in TIMEREGIONS, moving to months')
            try:
                seas = _MONTHS_[season]['month'] # [1] or [2] or ...
                LOGGER.info('Season to grep from MONTHS: %s ' % season)
                LOGGER.info('Season N to grep from MONTHS: %s ' % seas)
            except:
                seas = [1,2,3,4,5,6,7,8,9,10,11,12]
        ind = []

        # TODO: change concat_vals[i][0][4:6] to dt_obj.month !!!
        for i in range(len(res_times)):
            if (int(concat_vals[i][0][4:6]) in seas[:]):
                ind.append(i)
        sf = column_stack([concat_vals[i] for i in ind]).T
        seas_dim_filename = season + '_' + dim_filename
        savetxt(seas_dim_filename, sf, fmt='%s', delimiter=',')

        # -------------------------- plot with R ---------------
        R_plot_file = 'plot_csv.R'
        ld2_pdf = 'local_dims.pdf'
        ld2_seas_pdf = season + '_local_dims.pdf'

        args = ['Rscript', path.join(Rsrc, R_plot_file),
                '%s' % dim_filename,
                '%s' % ld2_pdf]
        try:
            output, error = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            LOGGER.info('R outlog info:\n %s ' % output)
            LOGGER.exception('R outlog errors:\n %s ' % error)
        except:
            msg = 'Could not produce plot'
            LOGGER.exception(msg)
            # TODO: Here need produce empty pdf to pass to output

        args = ['Rscript', path.join(Rsrc, R_plot_file),
                '%s' % seas_dim_filename,
                '%s' % ld2_seas_pdf]
        try:
            output, error = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            LOGGER.info('R outlog info:\n %s ' % output)
            LOGGER.exception('R outlog errors:\n %s ' % error)
        except:
            msg = 'Could not produce plot'
            LOGGER.exception(msg)
            # TODO: Here need produce empty pdf(s) to pass to output
        # 

        # ====================================================
        response.update_status('preparing output', 80)

        response.outputs['ldist'].file = dim_filename
        response.outputs['ldist_seas'].file = seas_dim_filename
        response.outputs['ld_pdf'].file = ld_pdf
        response.outputs['ld2_pdf'].file = ld2_pdf
        response.outputs['ld2_seas_pdf'].file = ld2_seas_pdf

        response.update_status('execution ended', 100)
        LOGGER.debug("total execution took %s seconds.",
                     time.time() - process_start_time)
        return response
