import os

from os.path import join, abspath, dirname, getsize, curdir, isfile

from datetime import date
from datetime import datetime as dt
import time  # performance test
import subprocess
from subprocess import CalledProcessError
import uuid
import psutil

from netCDF4 import Dataset

from numpy import squeeze

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexInput, ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata
# from pywps.inout.storage import FileStorage

from blackswan.datafetch import _PRESSUREDATA_
from blackswan.datafetch import reanalyses as rl
from blackswan.ocgis_module import call
from blackswan import analogs

# from blackswan.utils import rename_complexinputs
from blackswan.utils import get_variable, get_files_size

from blackswan.calculation import remove_mean_trend
from blackswan.log import init_process_logger

import logging
LOGGER = logging.getLogger("PYWPS")


class EventAttributionProcess(Process):
    def __init__(self):
        inputs = [

            LiteralInput("reanalyses", "Reanalyses Data",
                         abstract="Choose a reanalyses dataset for comparison",
                         default="NCEP_slp",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=_PRESSUREDATA_
                         ),

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

            LiteralInput('dateSt', 'Start date of event analysis period',
                         data_type='date',
                         abstract='First day of the period to be analysed',
                         default='2018-01-01',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('dateEn', 'End date of event analysis period',
                         data_type='date',
                         abstract='Last day of the period to be analysed',
                         default='2018-01-31',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('refSt1', 'Start date of reference period 1',
                         data_type='date',
                         abstract='First day of the period 1 where analogues being picked',
                         default='1948-01-01',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('refEn1', 'End date of reference period 1',
                         data_type='date',
                         abstract='Last day of the period 1 where analogues being picked',
                         default='1980-12-31',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('refSt2', 'Start date of reference period 2',
                         data_type='date',
                         abstract='First day of the period 2 where analogues being picked',
                         default='1981-01-01',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('refEn2', 'End date of reference period 2',
                         data_type='date',
                         abstract='Last day of the period 2 where analogues being picked',
                         default='2016-12-31',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput("detrend", "Detrend",
                         abstract="Remove long-term trend beforehand",
                         default='None',
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         allowed_values=['None', 'UVSpline']
                         ),

            LiteralInput("normalize", "normalization",
                         abstract="Normalize by subtraction of annual cycle",
                         default='None',
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         allowed_values=['None', 'base', 'sim', 'own']
                         ),

            LiteralInput("seasonwin", "Seasonal window",
                         abstract="Number of days before and after the date to be analysed",
                         default='30',
                         data_type='integer',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput("nanalog", "Nr of analogues",
                         abstract="Number of analogues to be detected",
                         default='20',
                         data_type='integer',
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

            LiteralInput("outformat", "output file format",
                         abstract="Choose the format for the analogue output file",
                         default="ascii",
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         allowed_values=['ascii', 'netCDF4']
                         ),

            LiteralInput("timewin", "Time window",
                         abstract="Number of days following the analogue day the distance will be averaged",
                         default='1',
                         data_type='integer',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput("plot", "Plot",
                         abstract="Plot simulations and Mean/Best/Last analogs?",
                         default='No',
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         allowed_values=['Yes', 'No']
                         ),
        ]

        outputs = [
            ComplexOutput("analog_pdf1", "Maps with mean analogs and simulation",
                          abstract="Analogs Maps",
                          supported_formats=[Format('image/pdf')],
                          as_reference=True,
                          ),

            ComplexOutput("analog_pdf2", "Maps with mean analogs and simulation",
                          abstract="Analogs Maps",
                          supported_formats=[Format('image/pdf')],
                          as_reference=True,
                          ),

            ComplexOutput("config1", "Config File for 1st period",
                          abstract="Config file used for the Fortran process",
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
                          ),

            ComplexOutput("config2", "Config File for 2nd period",
                          abstract="Config file used for the Fortran process",
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
                          ),

            ComplexOutput("analogs1", "Analogues File for 1st period",
                          abstract="mulit-column text file",
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
                          ),

            ComplexOutput("analogs2", "Analogues File for 2nd period",
                          abstract="mulit-column text file",
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
                          ),

            ComplexOutput("formated_analogs1", "Formated Analogues File for Ref1",
                          abstract="Formated analogues file for viewer",
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
                          ),

            ComplexOutput("formated_analogs2", "Formated Analogues File for Ref2",
                          abstract="Formated analogues file for viewer",
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
                          ),

            ComplexOutput('output_netcdf', 'Subsets for one dataset',
                          abstract="Prepared netCDF file as simulations (event period)",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput('target_netcdf1', 'Subsets for one dataset',
                          abstract="Prepared netCDF file as Reference 1 period",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput('target_netcdf2', 'Subsets for one dataset',
                          abstract="Prepared netCDF file as Reference 1 period",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput('base_netcdf1', 'Base Seasonal cycle from 1st ref period',
                          abstract="Base seasonal cycle netCDF",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput('base_netcdf2', 'Base Seasonal cycle from 2nd ref period',
                          abstract="Base seasonal cycle netCDF",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput('sim_netcdf1', 'Sim Seasonal cycle 1',
                          abstract="Sim seasonal cycle 1 netCDF",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput('sim_netcdf2', 'Sim Seasonal cycle 2',
                          abstract="Sim seasonal cycle 2 netCDF",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput("output1", "Analogues Viewer html page",
                          abstract="Interactive visualization of calculated analogues",
                          supported_formats=[Format("text/html")],
                          as_reference=True,
                          ),

            ComplexOutput("output2", "Analogues Viewer html page",
                          abstract="Interactive visualization of calculated analogues",
                          supported_formats=[Format("text/html")],
                          as_reference=True,
                          ),

            ComplexOutput('output_log', 'Logging information',
                          abstract="Collected logs during process run.",
                          as_reference=True,
                          supported_formats=[Format('text/plain')]
                          ),
        ]

        super(EventAttributionProcess, self).__init__(
            self._handler,
            identifier="events_attribution",
            title="Attribution of extreme event with analogues with reanalyses data",
            abstract='Attribution of extreme event using analogues of circulation (based on reanalyses data)',
            version="0.10",
            metadata=[
                Metadata('LSCE', 'http://www.lsce.ipsl.fr/en/index.php'),
                Metadata('Doc', 'http://blackswan.readthedocs.io/en/latest/'),
            ],
            inputs=inputs,
            outputs=outputs,
            status_supported=True,
            store_supported=True,
        )

    def _handler(self, request, response):

        LOGGER.debug('CURDIR XXXX : %s ' % (abspath(curdir)))
        LOGGER.debug('WORKDIR XXXX : %s ' % (self.workdir))
        os.chdir(self.workdir)
        LOGGER.debug('CURDIR XXXX : %s ' % (abspath(curdir)))

        init_process_logger('log.txt')

        LOGGER.info('Start process')
        response.update_status('execution started at : {}'.format(dt.now()), 5)

        process_start_time = time.time()  # measure process execution time ...
        start_time = time.time()  # measure init ...

        ################################
        # reading in the input arguments
        ################################

        try:
            response.update_status('read input parameter : %s ' % dt.now(), 6)

            refSt1 = request.inputs['refSt1'][0].data
            refEn1 = request.inputs['refEn1'][0].data

            refSt2 = request.inputs['refSt2'][0].data
            refEn2 = request.inputs['refEn2'][0].data

            dateSt = request.inputs['dateSt'][0].data
            dateEn = request.inputs['dateEn'][0].data

            seasonwin = request.inputs['seasonwin'][0].data
            nanalog = request.inputs['nanalog'][0].data

            # timres = request.inputs['timeres'][0].data
            timres = 'day'

            bboxDef = '-20,40,30,70'  # in general format

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

            bbox.append(float(bboxStr[0]))
            bbox.append(float(bboxStr[2]))
            bbox.append(float(bboxStr[1]))
            bbox.append(float(bboxStr[3]))
            LOGGER.debug('BBOX for ocgis: %s ' % (bbox))
            LOGGER.debug('BBOX original: %s ' % (bboxStr))

            normalize = request.inputs['normalize'][0].data
            detrend = request.inputs['detrend'][0].data
            plot = request.inputs['plot'][0].data
            distance = request.inputs['dist'][0].data
            outformat = request.inputs['outformat'][0].data
            timewin = request.inputs['timewin'][0].data

            model_var = request.inputs['reanalyses'][0].data
            model, var = model_var.split('_')

            LOGGER.info('input parameters set')
            response.update_status('Read in and convert the arguments', 7)
        except Exception as e:
            msg = 'failed to read input prameter %s ' % e
            LOGGER.exception(msg)
            raise Exception(msg)

        ######################################
        # convert types and set environment
        ######################################

        try:
            response.update_status('Preparing enviroment converting arguments', 8)
            LOGGER.debug('dates: %s %s %s %s %s %s %s' % (type(refSt1), refSt1, refEn1, refSt2, refEn2, dateSt, dateEn))

            start = min(refSt1, refSt2, dateSt)
            end = max(refEn1, refEn2, dateEn)

            if normalize == 'None':
                seacyc = False
            else:
                seacyc = True

            if outformat == 'ascii':
                outformat = '.txt'
            elif outformat == 'netCDF':
                outformat = '.nc'
            else:
                LOGGER.exception('output format not valid')

        except Exception as e:
            msg = 'failed to set environment %s ' % e
            LOGGER.exception(msg)
            raise Exception(msg)

        ###########################
        # set the environment
        ###########################

        response.update_status('fetching data from archive', 9)

        try:
            if model == 'NCEP':
                getlevel = False
                if 'z' in var:
                    level = var.strip('z')
                    # conform_units_to = None
                else:
                    level = None
                    if var == 'precip':
                        var = 'pr_wtr'
                    # conform_units_to = 'hPa'
            elif '20CRV2' in model:
                getlevel = False
                if 'z' in var:
                    level = var.strip('z')
                    # conform_units_to = None
                else:
                    level = None
                    # conform_units_to = 'hPa'
            else:
                LOGGER.exception('Reanalyses dataset not known')
            LOGGER.info('environment set for model: %s' % model)
        except Exception:
            msg = 'failed to set environment'
            LOGGER.exception(msg)
            raise Exception(msg)


        ##########################################
        # fetch Data from original data archive
        ##########################################

        # NOTE: If ref is say 1950 - 1990, and sim is just 1 week in 2017:
        # - ALL the data will be downloaded, 1950 - 2017
        try:
            model_nc = rl(start=start.year,
                          end=end.year,
                          dataset=model,
                          variable=var, timres=timres, getlevel=getlevel)
            LOGGER.info('reanalyses data fetched')
        except Exception:
            msg = 'failed to get reanalyses data'
            LOGGER.exception(msg)
            raise Exception(msg)

        response.update_status('subsetting region of interest', 10)

        LOGGER.debug("start and end time: %s - %s" % (start, end))
        time_range = [start, end]

        # Checking memory and dataset size
        model_size = get_files_size(model_nc)
        memory_avail = psutil.virtual_memory().available
        thrs = 0.3  # 30%
        if (model_size >= thrs * memory_avail):
            ser_r = True
        else:
            ser_r = False

        # ################################
        # For 20CRV2 geopotential height, daily dataset for 100 years is about 50 Gb
        # So it makes sense, to operate it step-by-step
        # TODO: need to create dictionary for such datasets (for models as well)
        # TODO: benchmark the method bellow for NCEP z500 for 60 years

        if ('z' in var):
            tmp_total = []
            origvar = get_variable(model_nc)

            for z in model_nc:
                # tmp_n = 'tmp_%s' % (uuid.uuid1())
                b0 = call(resource=z, variable=origvar, level_range=[int(level), int(level)], geom=bbox,
                spatial_wrapping='wrap', prefix='levdom_' + os.path.basename(z)[0:-3])
                tmp_total.append(b0)

            tmp_total = sorted(tmp_total, key=lambda i: os.path.splitext(os.path.basename(i))[0])
            inter_subset_tmp = call(resource=tmp_total, variable=origvar, time_range=time_range)

            # Clean
            for i in tmp_total:
                tbr = 'rm -f %s' % (i)
                os.system(tbr)

            # Create new variable
            ds = Dataset(inter_subset_tmp, mode='a')
            z_var = ds.variables.pop(origvar)
            dims = z_var.dimensions
            new_var = ds.createVariable('z%s' % level, z_var.dtype, dimensions=(dims[0], dims[2], dims[3]))
            new_var[:, :, :] = squeeze(z_var[:, 0, :, :])
            # new_var.setncatts({k: z_var.getncattr(k) for k in z_var.ncattrs()})
            ds.close()
            model_subset_tmp = call(inter_subset_tmp, variable='z%s' % level)
        else:
            if ser_r:
                LOGGER.debug('Process reanalysis step-by-step')
                tmp_total = []
                for z in model_nc:
                    # tmp_n = 'tmp_%s' % (uuid.uuid1())
                    b0 = call(resource=z, variable=var, geom=bbox, spatial_wrapping='wrap',
                            prefix='Rdom_' + os.path.basename(z)[0:-3])
                    tmp_total.append(b0)
                tmp_total = sorted(tmp_total, key=lambda i: os.path.splitext(os.path.basename(i))[0])
                model_subset_tmp = call(resource=tmp_total, variable=var, time_range=time_range)
            else:
                LOGGER.debug('Using whole dataset at once')
                model_subset_tmp = call(resource=model_nc, variable=var,
                                        geom=bbox, spatial_wrapping='wrap', time_range=time_range,
                                        )

        # If dataset is 20CRV2 the 6 hourly file should be converted to daily.
        # Option to use previously 6h data from cache (if any) and not download daily files.

        # Disabled for now
        # if '20CRV2' in model:
        #     if timres == '6h':
        #         from cdo import Cdo

        #         cdo = Cdo(env=os.environ)
        #         model_subset = '%s.nc' % uuid.uuid1()
        #         tmp_f = '%s.nc' % uuid.uuid1()

        #         cdo_op = getattr(cdo, 'daymean')
        #         cdo_op(input=model_subset_tmp, output=tmp_f)
        #         sti = '00:00:00'
        #         cdo_op = getattr(cdo, 'settime')
        #         cdo_op(sti, input=tmp_f, output=model_subset)
        #         LOGGER.debug('File Converted from: %s to daily' % (timres))
        #     else:
        #         model_subset = model_subset_tmp
        # else:
        #     model_subset = model_subset_tmp

        # Uncomment \/\/\/ if work with 6h data...
        model_subset = model_subset_tmp

        LOGGER.info('Dataset subset done: %s ', model_subset)
        response.update_status('dataset subsetted', 15)

        # BLOCK OF DETRENDING of model_subset !
        # Original model subset kept to further visualisaion if needed
        # Now is issue with SLP:
        # TODO 1 Keep trend as separate file
        # TODO 2 Think how to add options to plot abomalies AND original data...
        #        May be do archive and simulation = call.. over NOT detrended data and keep it as well
        # TODO 3 Check with faster smoother add removing trend of each grid

        if detrend == 'None':
            orig_model_subset = model_subset
        else:
            orig_model_subset = remove_mean_trend(model_subset, varname=var)

        # ======================================

        LOGGER.debug("get_input_subset_dataset took %s seconds.",
                     time.time() - start_time)
        response.update_status('**** Input data fetched', 20)

        ########################
        # input data preperation
        ########################

        response.update_status('Start preparing input data', 30)
        start_time = time.time()  # measure data preperation ...

        try:
            # Construct descriptive filenames for the files
            # to be listed in config file

            # Fix < 1900 issue...
            refDatesString1 = refSt1.isoformat().strip().split("T")[0] + "_" + refEn1.isoformat().strip().split("T")[0]
            refDatesString2 = refSt2.isoformat().strip().split("T")[0] + "_" + refEn2.isoformat().strip().split("T")[0]
            simDatesString = dateSt.isoformat().strip().split("T")[0] + "_" + dateEn.isoformat().strip().split("T")[0]

            archiveNameString1 = "base1_" + var + "_" + refDatesString1 + '_%.1f_%.1f_%.1f_%.1f' \
                                % (bbox[0], bbox[2], bbox[1], bbox[3])
            archiveNameString2 = "base2_" + var + "_" + refDatesString2 + '_%.1f_%.1f_%.1f_%.1f' \
                                % (bbox[0], bbox[2], bbox[1], bbox[3])

            simNameString = "sim_" + var + "_" + simDatesString + '_%.1f_%.1f_%.1f_%.1f' \
                            % (bbox[0], bbox[2], bbox[1], bbox[3])

            archive1 = call(resource=model_subset,
                           time_range=[refSt1, refEn1],
                           prefix=archiveNameString1)
            archive2 = call(resource=model_subset,
                           time_range=[refSt2, refEn2],
                           prefix=archiveNameString2)

            simulation = call(resource=model_subset, time_range=[dateSt, dateEn],
                              prefix=simNameString)
            LOGGER.info('archive and simulation files generated: %s, %s'
                        % (archive1, simulation))
            LOGGER.info('archive and simulation files generated: %s, %s'
                        % (archive2, simulation))
        except Exception as e:
            msg = 'failed to prepare archive and simulation files %s ' % e
            LOGGER.exception(msg)
            raise Exception(msg)

        try:
            if seacyc is True:
                LOGGER.info('normalization function with method: %s '
                            % normalize)
                seasoncyc_base1, seasoncyc_sim1 = analogs.seacyc(
                    archive1,
                    simulation, basecyc='seasoncyc_base1.nc', simcyc='seasoncyc_sim1.nc',
                    method=normalize)
                seasoncyc_base2, seasoncyc_sim2 = analogs.seacyc(
                    archive2,
                    simulation, basecyc='seasoncyc_base2.nc', simcyc='seasoncyc_sim2.nc',
                    method=normalize)
            else:
                seasoncyc_base1 = seasoncyc_base2 = seasoncyc_sim1 = seasoncyc_sim2 = None
        except Exception as e:
            msg = 'failed to generate normalization files %s ' % e
            LOGGER.exception(msg)
            raise Exception(msg)

        output_file1 = 'output1.txt'
        output_file2 = 'output2.txt'
        files1 = [os.path.abspath(archive1), os.path.abspath(simulation), output_file1]
        files2 = [os.path.abspath(archive2), os.path.abspath(simulation), output_file2]
        LOGGER.debug("Data preperation took %s seconds.",
                     time.time() - start_time)

        ############################
        # generate the config files
        ############################

        config_file1 = analogs.get_configfile(
            files=files1,
            seasoncyc_base=seasoncyc_base1,
            seasoncyc_sim=seasoncyc_sim1,
            base_id=model,
            sim_id=model,
            timewin=timewin,
            varname=var,
            seacyc=seacyc,
            cycsmooth=91,
            nanalog=nanalog,
            seasonwin=seasonwin,
            distfun=distance,
            outformat=outformat,
            calccor=True,
            silent=False,
            # period=[dt.strftime(refSt, '%Y-%m-%d'), dt.strftime(refEn, '%Y-%m-%d')],
            period=[refSt1.isoformat().strip().split("T")[0], refEn1.isoformat().strip().split("T")[0]],
            bbox="{0[0]},{0[2]},{0[1]},{0[3]}".format(bbox), config_file = 'config1.txt')

        config_file2 = analogs.get_configfile(
            files=files2,
            seasoncyc_base=seasoncyc_base2,
            seasoncyc_sim=seasoncyc_sim2,
            base_id=model,
            sim_id=model,
            timewin=timewin,
            varname=var,
            seacyc=seacyc,
            cycsmooth=91,
            nanalog=nanalog,
            seasonwin=seasonwin,
            distfun=distance,
            outformat=outformat,
            calccor=True,
            silent=False,
            # period=[dt.strftime(refSt, '%Y-%m-%d'), dt.strftime(refEn, '%Y-%m-%d')],
            period=[refSt2.isoformat().strip().split("T")[0], refEn2.isoformat().strip().split("T")[0]],
            bbox="{0[0]},{0[2]},{0[1]},{0[3]}".format(bbox), config_file = 'config2.txt')

        response.update_status('generated config file', 40)
        #######################
        # CASTf90 call
        #######################

        # -----------------------
        try:
            import ctypes
            # TODO: This lib is for linux
            mkl_rt = ctypes.CDLL('libmkl_rt.so')
            nth = mkl_rt.mkl_get_max_threads()
            LOGGER.debug('Current number of threads: %s' % (nth))
            mkl_rt.mkl_set_num_threads(ctypes.byref(ctypes.c_int(64)))
            nth = mkl_rt.mkl_get_max_threads()
            LOGGER.debug('NEW number of threads: %s' % (nth))
            # TODO: Does it \/\/\/ work with default shell=False in subprocess... (?)
            os.environ['MKL_NUM_THREADS'] = str(nth)
            os.environ['OMP_NUM_THREADS'] = str(nth)
        except Exception as e:
            msg = 'Failed to set THREADS %s ' % e
            LOGGER.debug(msg)
        # -----------------------

        # ##### TEMPORAL WORKAROUND! With instaled hdf5-1.8.18 in anaconda ###############
        # ##### MUST be removed after castf90 recompiled with the latest hdf version
        # ##### NOT safe
        os.environ['HDF5_DISABLE_VERSION_CHECK'] = '1'
        # hdflib = os.path.expanduser("~") + '/anaconda/lib'
        # hdflib = os.getenv("HOME") + '/anaconda/lib'
        import pwd
        hdflib = pwd.getpwuid(os.getuid()).pw_dir + '/anaconda/lib'
        os.environ['LD_LIBRARY_PATH'] = hdflib
        # ################################################################################

        response.update_status('Start CASTf90 call for 1st ref period', 50)
        try:
            cmd = ['analogue.out', config_file1]
            LOGGER.debug("castf90 command: %s", cmd)
            output1 = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            LOGGER.info('analogue output:\n %s', output1)
            response.update_status('**** CASTf90 1st suceeded', 60)

            cmd = ['analogue.out', config_file2]
            LOGGER.debug("castf90 command: %s", cmd)
            output2 = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            LOGGER.info('analogue output:\n %s', output2)
            response.update_status('**** CASTf90 2nd suceeded', 70)

        except CalledProcessError as e:
            msg = 'CASTf90 failed:\n{0}'.format(e.output)
            LOGGER.exception(msg)
            raise Exception(msg)

        LOGGER.debug("castf90 took %s seconds.", time.time() - start_time)

        # TODO: Add try - except for pdfs

        if plot == 'Yes':
            analogs_pdf1 = analogs.plot_analogs(configfile=config_file1, soutpdf = 'Analogs1.pdf')
            analogs_pdf2 = analogs.plot_analogs(configfile=config_file2, soutpdf = 'Analogs2.pdf')
        else:
            analogs_pdf1 = 'dummy_plot1.pdf'
            with open(analogs_pdf1, 'a'): os.utime(analogs_pdf1, None)
            analogs_pdf2 = 'dummy_plot2.pdf'
            with open(analogs_pdf2, 'a'): os.utime(analogs_pdf2, None)

        response.update_status('preparing output', 80)

        response.outputs['analog_pdf1'].file = analogs_pdf1
        response.outputs['analog_pdf2'].file = analogs_pdf2

        response.outputs['config1'].file = config_file1
        response.outputs['config2'].file = config_file2

        response.outputs['analogs1'].file = output_file1
        response.outputs['analogs2'].file = output_file2

        response.outputs['output_netcdf'].file = simulation

        response.outputs['target_netcdf1'].file = archive1
        response.outputs['target_netcdf2'].file = archive2

        if seacyc is True:
            response.outputs['base_netcdf1'].file = seasoncyc_base1
            response.outputs['sim_netcdf1'].file = seasoncyc_sim1
            response.outputs['base_netcdf2'].file = seasoncyc_base2
            response.outputs['sim_netcdf2'].file = seasoncyc_sim2
        else:
            # TODO: Still unclear how to overpass unknown number of outputs
            dummy_base1 = 'dummy_base1.nc'
            dummy_sim1 = 'dummy_sim1.nc'
            dummy_base2 = 'dummy_base2.nc'
            dummy_sim2 = 'dummy_sim2.nc'
            with open(dummy_base1, 'a'): os.utime(dummy_base1, None)
            with open(dummy_sim1, 'a'): os.utime(dummy_sim1, None)
            with open(dummy_base2, 'a'): os.utime(dummy_base2, None)
            with open(dummy_sim2, 'a'): os.utime(dummy_sim2, None)
            response.outputs['base_netcdf1'].file = dummy_base1
            response.outputs['sim_netcdf1'].file = dummy_sim1
            response.outputs['base_netcdf2'].file = dummy_base2
            response.outputs['sim_netcdf2'].file = dummy_sim2

        ########################
        # generate analog viewer
        ########################

        formated_analogs_file1 = analogs.reformat_analogs(output_file1, prefix='modified-analogfile1.tsv')
        response.outputs['formated_analogs1'].file = formated_analogs_file1
        LOGGER.info('analogs 1 reformated')

        viewer_html1 = analogs.render_viewer(
            configfile=config_file1,
            datafile=formated_analogs_file1, outhtml='analogviewer1.html')
        response.outputs['output1'].file = viewer_html1

        formated_analogs_file2 = analogs.reformat_analogs(output_file1, prefix='modified-analogfile2.tsv')
        response.outputs['formated_analogs2'].file = formated_analogs_file2
        LOGGER.info('analogs 1 reformated')

        viewer_html2 = analogs.render_viewer(
            configfile=config_file2,
            datafile=formated_analogs_file2, outhtml='analogviewer2.html')
        response.outputs['output2'].file = viewer_html2

        response.update_status('Successfully generated analogs viewers', 90)
        LOGGER.info('rendered pages: %s ', viewer_html1)
        LOGGER.info('rendered pages: %s ', viewer_html2)

        response.update_status('execution ended', 100)
        LOGGER.debug("total execution took %s seconds.",
                     time.time() - process_start_time)
        response.outputs['output_log'].file = 'log.txt'
        return response

