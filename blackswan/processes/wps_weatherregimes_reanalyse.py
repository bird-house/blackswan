"""
Processes for Weather Classification
Author: Nils Hempelmann (nils.hempelmann@lsce.ipsl.fr)
"""

from blackswan.datafetch import _PRESSUREDATA_
from blackswan.weatherregimes import _TIMEREGIONS_
from pywps import Process
from pywps import LiteralInput
from pywps import ComplexInput, ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata
from blackswan.log import init_process_logger

from blackswan.ocgis_module import call
from os.path import basename, splitext

import logging
LOGGER = logging.getLogger("PYWPS")

def ocgis_call_wrap(tmargs):
    _z=tmargs[0]
    _origvar=tmargs[1]
    _level=tmargs[2]
    _bbox=tmargs[3]
    _plev=[int(_level), int(_level)]
    _pref='levdom_'+basename(_z)[0:-3]

    _tmpf=call(resource=_z, variable=_origvar, level_range=_plev, geom=_bbox,
                spatial_wrapping='wrap', prefix=_pref)

    return _tmpf

class WeatherregimesreanalyseProcess(Process):
    def __init__(self):
        inputs = [

            # self.BBox = self.addBBoxInput(
            #     identifier="BBox",
            #     title="Bounding Box",
            #     abstract="coordinates to define the region for weather classification ('EPSG:4326')",
            #     minOccurs=1,
            #     maxOccurs=1,
            #     crss=['EPSG:4326']
            #     )

            # Literal Input Data
            # ------------------

            # self.BBox = self.addLiteralInput(
            #     identifier="BBox",
            #     title="Region",
            #     abstract="coordinates to define the region: (minlon,maxlon,minlat,maxlat)",
            #     default='-80,22.5,50,70', #  cdo syntax: 'minlon,maxlon,minlat,maxlat' ;
            #  ocgis syntax (minlon,minlat,maxlon,maxlat)
            #     type=type(''),
            #     minOccurs=1,
            #     maxOccurs=1,
            #     )

            LiteralInput("season", "Time region",
                         abstract="Select the months to define the time region (all == whole year will be analysed)",
                         default="DJF",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=_TIMEREGIONS_.keys()
                         ),

            LiteralInput('BBox', 'Bounding Box',
                         data_type='string',
                         abstract="Enter a bbox: min_lon, max_lon, min_lat, max_lat."
                            " min_lon=Western longitude,"
                            " max_lon=Eastern longitude,"
                            " min_lat=Southern or northern latitude,"
                            " max_lat=Northern or southern latitude."
                            " For example: -80,50,20,70",
                         min_occurs=1,
                         max_occurs=1,
                         default='-80,50,20,70',
                         ),

            LiteralInput("period", "Period for weatherregime calculation",
                         abstract="Period for analysing the dataset",
                         default="19700101-20101231",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         ),

            LiteralInput("anualcycle", "Period for anualcycle calculation",
                         abstract="Period for anual cycle calculation",
                         default="19700101-19991231",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         ),

            LiteralInput("reanalyses", "Reanalyses Data",
                         abstract="Choose a reanalyses dataset for comparison",
                         default="NCEP_slp",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=_PRESSUREDATA_
                         ),

            LiteralInput("method", "Method of annual cycle calculation",
                         abstract="Method of annual cycle calculation",
                         default="ocgis",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=['ocgis', 'cdo']
                         ),

            LiteralInput("sseas", "Serial or multiprocessing for annual cycle",
                         abstract="Serial or multiprocessing for annual cycle",
                         default="multi",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=['serial', 'multi']
                         ),

            LiteralInput("kappa", "Nr of Weather regimes",
                         abstract="Set the number of clusters to be detected",
                         default='4',
                         data_type='integer',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=range(2, 11)
                         ),
        ]

        outputs = [
            ComplexOutput("Routput_graphic", "Weather Regime Pressure map",
                          abstract="Weather Classification",
                          supported_formats=[Format('image/pdf')],
                          as_reference=True,
                          ),

            ComplexOutput("output_pca", "R - datafile",
                          abstract="Principal components (PCA)",
                          supported_formats=[Format('text/plain')],
                          as_reference=True,
                          ),

            ComplexOutput("output_classification", "R - workspace",
                          abstract="Weather regime classification",
                          supported_formats=[Format("application/octet-stream")],
                          as_reference=True,
                          ),

            ComplexOutput('output_netcdf', 'Subsets for one dataset',
                          abstract="Prepared netCDF file as input for weatherregime calculation",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput('output_log', 'Logging information',
                          abstract="Collected logs during process run.",
                          as_reference=True,
                          supported_formats=[Format('text/plain')]
                          ),
        ]

        super(WeatherregimesreanalyseProcess, self).__init__(
            self._handler,
            identifier="weatherregimes_reanalyse",
            title="Weather Regimes (based on reanalyses data)",
            abstract='k-mean cluster analyse of the pressure patterns. Clusters are equivalent to weather regimes',
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
        from datetime import datetime as dt
        from blackswan import weatherregimes as wr
        from tempfile import mkstemp

        response.update_status('execution started at : {}'.format(dt.now()), 5)

        ################################
        # reading in the input arguments
        ################################
        LOGGER.info('read in the arguments')
        # resources = self.getInputValues(identifier='resources')
        season = request.inputs['season'][0].data
        LOGGER.info('season %s', season)

        #bbox = [-80, 20, 50, 70]
        # TODO: Add checking for wrong cordinates and apply default if nesessary
        bbox = []
        bboxStr = request.inputs['BBox'][0].data
        bboxStr = bboxStr.split(',')
        bbox.append(float(bboxStr[0]))
        bbox.append(float(bboxStr[2]))
        bbox.append(float(bboxStr[1]))
        bbox.append(float(bboxStr[3]))
        LOGGER.debug('BBOX for ocgis: %s ' % (bbox))
        LOGGER.debug('BBOX original: %s ' % (bboxStr))

        model_var = request.inputs['reanalyses'][0].data
        model, variable = model_var.split('_')

        period = request.inputs['period'][0].data
        LOGGER.info('period %s', period)
        anualcycle = request.inputs['anualcycle'][0].data
        kappa = request.inputs['kappa'][0].data
        LOGGER.info('kappa %s', kappa)

        method = request.inputs['method'][0].data
        LOGGER.info('Calc annual cycle with %s', method)

        sseas = request.inputs['sseas'][0].data
        LOGGER.info('Annual cycle calc with %s', sseas)

        start = dt.strptime(period.split('-')[0], '%Y%m%d')
        end = dt.strptime(period.split('-')[1], '%Y%m%d')
        LOGGER.debug('start: %s , end: %s ' % (start, end))

        ###########################
        # set the environment
        ###########################

        response.update_status('fetching data from archive', 10)

        try:
            if model == 'NCEP':
                getlevel = False
                if 'z' in variable:
                    level = variable.strip('z')
                    conform_units_to = None
                else:
                    level = None
                    conform_units_to = 'hPa'
            elif '20CRV2' in model:
                getlevel = False
                if 'z' in variable:
                    level = variable.strip('z')
                    conform_units_to = None
                else:
                    level = None
                    conform_units_to = 'hPa'
            else:
                LOGGER.exception('Reanalyses dataset not known')
            LOGGER.info('environment set for model: %s' % model)
        except:
            msg = 'failed to set environment'
            LOGGER.exception(msg)
            raise Exception(msg)

        ##########################################
        # fetch Data from original data archive
        ##########################################

        from blackswan.datafetch import reanalyses as rl
        from blackswan.utils import get_variable
        # from os.path import basename, splitext
        from os import system
        from netCDF4 import Dataset
        from numpy import squeeze

        try:
            model_nc = rl(start=start.year,
                          end=end.year,
                          dataset=model,
                          variable=variable,
                          getlevel=getlevel)
            LOGGER.info('reanalyses data fetched')
        except:
            msg = 'failed to get reanalyses data'
            LOGGER.exception(msg)
            raise Exception(msg)

        response.update_status('fetching data done', 15)
        ############################################################
        # get the required bbox and time region from resource data
        ############################################################

        response.update_status('subsetting region of interest', 17)
        # from flyingpigeon.weatherregimes import get_level
        # from flyingpigeon.ocgis_module import call

        time_range = [start, end]

        ############################################################
        # Block of level and domain selection for geop huge dataset
        ############################################################

        LevMulti = False

        # ===========================================================================================
        if ('z' in variable):  
            tmp_total = []
            origvar = get_variable(model_nc)

            if (LevMulti == False):
                for z in model_nc:
                    b0 = call(resource=z, variable=origvar, level_range=[int(level), int(level)], geom=bbox,
                    spatial_wrapping='wrap', prefix='levdom_'+basename(z)[0:-3])
                    tmp_total.append(b0)
            else:
                # multiproc - no inprovements yet, need to check in hi perf machine...
                #-----------------------
                try:
                    import ctypes
                    import os
                    # TODO: This lib is for linux
                    mkl_rt = ctypes.CDLL('libmkl_rt.so')
                    nth = mkl_rt.mkl_get_max_threads()
                    LOGGER.debug('Current number of threads: %s' % (nth))
                    mkl_rt.mkl_set_num_threads(ctypes.byref(ctypes.c_int(64)))
                    nth = mkl_rt.mkl_get_max_threads()
                    LOGGER.debug('NEW number of threads: %s' % (nth))
                    # TODO: Does it \/\/\/ work with default shell=False in subprocess... (?)
                    os.environ['MKL_NUM_THREADS']=str(nth)
                    os.environ['OMP_NUM_THREADS']=str(nth)
                except Exception as e:
                    msg = 'Failed to set THREADS %s ' % e
                    LOGGER.debug(msg)
                #-----------------------

                from multiprocessing import Pool
                pool = Pool()
                # from multiprocessing.dummy import Pool as ThreadPool
                #pool = ThreadPool()
                tup_var = [origvar] * len(model_nc)
                tup_lev = [level] * len(model_nc)
                tup_bbox = [bbox] * len(model_nc)
                tup_args = zip(model_nc, tup_var, tup_lev, tup_bbox)

                tmp_total = pool.map(ocgis_call_wrap, tup_args)
                pool.close()
                pool.join()

            LOGGER.debug('Temporal subset files: %s'%(tmp_total))

            tmp_total = sorted(tmp_total, key=lambda i: splitext(basename(i))[0])
            inter_subset_tmp = call(resource=tmp_total, variable=origvar, time_range=time_range)

            # Clean
            for i in tmp_total:
                tbr = 'rm -f %s' % (i)
                system(tbr)

            # Create new variable
            ds = Dataset(inter_subset_tmp, mode='a')
            z_var = ds.variables.pop(origvar)
            dims = z_var.dimensions
            new_var = ds.createVariable('z%s' % level, z_var.dtype, dimensions=(dims[0], dims[2], dims[3]))
            new_var[:, :, :] = squeeze(z_var[:, 0, :, :])
            # new_var.setncatts({k: z_var.getncattr(k) for k in z_var.ncattrs()})
            ds.close()
            model_subset = call(inter_subset_tmp, variable='z%s' % level)
        else:
            model_subset = call(resource=model_nc, variable=variable,
                                geom=bbox, spatial_wrapping='wrap', time_range=time_range,
                                # conform_units_to=conform_units_to
                                )
        # =============================================================================================
        LOGGER.info('Dataset subset done: %s ', model_subset)

        response.update_status('dataset subsetted', 18)
        ##############################################
        # computing anomalies
        ##############################################
        response.update_status('computing anomalies ', 19)

        cycst = anualcycle.split('-')[0]
        cycen = anualcycle.split('-')[1]
        reference = [dt.strptime(cycst, '%Y%m%d'), dt.strptime(cycen, '%Y%m%d')]
        LOGGER.info('reference time: %s', reference)

        model_anomal = wr.get_anomalies(model_subset, reference=reference, method=method, sseas=sseas) #, variable=variable)

        #####################
        # extracting season
        #####################
        response.update_status('normalizing data', 21)
        model_season = wr.get_season(model_anomal, season=season)

        response.update_status('anomalies computed and  normalized', 24)
        #######################
        # call the R scripts
        #######################
        response.update_status('Start weather regime clustering ', 25)
        import shlex
        import subprocess
        from blackswan import config
        from os.path import curdir, exists, join

        try:
            rworkspace = curdir
            Rsrc = config.Rsrc_dir()
            Rfile = 'weatherregimes_model.R'

            infile = model_season  # model_subset #model_ponderate
            modelname = model
            yr1 = start.year
            yr2 = end.year
            ip, output_graphics = mkstemp(dir=curdir, suffix='.pdf')
            ip, file_pca = mkstemp(dir=curdir, suffix='.txt')
            ip, file_class = mkstemp(dir=curdir, suffix='.Rdat')

            args = ['Rscript', join(Rsrc, Rfile), '%s/' % curdir,
                    '%s/' % Rsrc, '%s' % infile, '%s' % variable,
                    '%s' % output_graphics, '%s' % file_pca,
                    '%s' % file_class, '%s' % season,
                    '%s' % start.year, '%s' % end.year,
                    '%s' % model_var, '%s' % kappa]
            LOGGER.info('Rcall builded')
            LOGGER.debug('ARGS: %s'%(args))
        except:
            msg = 'failed to build the R command'
            LOGGER.exception(msg)
            raise Exception(msg)
        try:
            output, error = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            LOGGER.info('R outlog info:\n %s ' % output)
            LOGGER.exception('R outlog errors:\n %s ' % error)
            if len(output) > 0:
                response.update_status('**** weatherregime in R suceeded', 90)
            else:
                LOGGER.exception('NO! output returned from R call')
        except:
            msg = 'weatherregime in R'
            LOGGER.exception(msg)
            raise Exception(msg)

        response.update_status('Weather regime clustering done ', 93)
        ############################################
        # set the outputs
        ############################################
        response.update_status('Set the process outputs ', 95)

        response.outputs['Routput_graphic'].file = output_graphics
        response.outputs['output_pca'].file = file_pca
        response.outputs['output_classification'].file = file_class
        response.outputs['output_netcdf'].file = model_subset
        response.update_status('done', 100)
        return response
