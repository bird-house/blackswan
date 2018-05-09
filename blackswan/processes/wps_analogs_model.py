import os
from os import path, environ, utime, getuid
from tempfile import mkstemp
from datetime import datetime as dt
# from datetime import timedelta as td
from datetime import time as dt_time
from datetime import date
import time  # performance test

# later goes to utils
from netCDF4 import Dataset

from blackswan.datafetch import _PRESSUREDATA_
from blackswan import analogs
from blackswan.ocgis_module import call
from blackswan.datafetch import get_level
from blackswan.utils import get_variable
from blackswan.utils import rename_complexinputs
from blackswan.utils import archiveextract
from blackswan.utils import get_timerange, get_calendar
from blackswan.calculation import remove_mean_trend

from pywps import Process
from pywps import LiteralInput, LiteralOutput
from pywps import ComplexInput, ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata
from blackswan.log import init_process_logger

import logging
LOGGER = logging.getLogger("PYWPS")


class AnalogsmodelProcess(Process):
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

            LiteralInput('dateSt', 'Start date of analysis period',
                         data_type='date',
                         abstract='First day of the period to be analysed',
                         default='2000-07-15',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('dateEn', 'End date of analysis period',
                         data_type='date',
                         abstract='Last day of the period to be analysed',
                         default='2000-07-18',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('refSt', 'Start date of reference period',
                         data_type='date',
                         abstract='First day of the period where analogues being picked',
                         default='1950-01-01',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('refEn', 'End date of reference period',
                         data_type='date',
                         abstract='Last day of the period where analogues being picked',
                         default='1999-12-31',
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
                         default='base',
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         allowed_values=['None', 'base', 'sim', 'own']
                         ),

            LiteralInput("seasonwin", "Seasonal window",
                         abstract="Number of days befor and after the date to be analysed",
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
            ComplexOutput("analog_pdf", "Maps with mean analogs and simulation",
                          abstract="Analogs Maps",
                          supported_formats=[Format('image/pdf')],
                          as_reference=True,
                          ),

            ComplexOutput("config", "Config File",
                          abstract="Config file used for the Fortran process",
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
                          ),

            ComplexOutput("analogs", "Analogues File",
                          abstract="mulit-column text file",
                          as_reference=True,
                          supported_formats=[Format("text/plain")],
                          ),

            ComplexOutput("formated_analogs", "Formated Analogues File",
                          abstract="Formated analogues file for viewer",
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
                          ),

            ComplexOutput('output_netcdf', 'Subsets for one dataset',
                          abstract="Prepared netCDF file as input for weatherregime calculation",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput('target_netcdf', 'Subsets for one dataset',
                          abstract="Prepared netCDF file as input for archive",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput('base_netcdf', 'Base Seasonal cycle',
                          abstract="Base seasonal cycle netCDF",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput('sim_netcdf', 'Sim Seasonal cycle',
                          abstract="Sim seasonal cycle netCDF",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]
                          ),

            ComplexOutput("output", "Analogues Viewer html page",
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

        super(AnalogsmodelProcess, self).__init__(
            self._handler,
            identifier="analogs_model",
            title="Analogues of circulation (based on climate model data)",
            abstract='Search for days with analogue pressure pattern for models data sets',
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
            refSt = request.inputs['refSt'][0].data
            refEn = request.inputs['refEn'][0].data
            dateSt = request.inputs['dateSt'][0].data
            dateEn = request.inputs['dateEn'][0].data
            seasonwin = request.inputs['seasonwin'][0].data
            nanalog = request.inputs['nanalog'][0].data

            bboxDef = '-20,40,30,70'  # in general format
            # level = 500

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

            normalize = request.inputs['normalize'][0].data
            plot = request.inputs['plot'][0].data
            distance = request.inputs['dist'][0].data
            outformat = request.inputs['outformat'][0].data
            timewin = request.inputs['timewin'][0].data
            detrend = request.inputs['detrend'][0].data

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
            refSt = dt.combine(refSt, dt_time(12, 0))
            refEn = dt.combine(refEn, dt_time(12, 0))
            dateSt = dt.combine(dateSt, dt_time(12, 0))
            dateEn = dt.combine(dateEn, dt_time(12, 0))

            # Check if 360_day calendar:
            try:
                if type(resource) is not list:
                    resource = [resource]

                modcal, calunits = get_calendar(resource[0])
                if '360_day' in modcal:
                    if refSt.day == 31:
                        refSt = refSt.replace(day=30)
                        LOGGER.debug('Date has been changed for: %s' % (refSt))
                    if refEn.day == 31:
                        refEn = refEn.replace(day=30)
                        LOGGER.debug('Date has been changed for: %s' % (refEn))
                    if dateSt.day == 31:
                        dateSt = dateSt.replace(day=30)
                        LOGGER.debug('Date has been changed for: %s' % (dateSt))
                    if dateEn.day == 31:
                        dateEn = dateEn.replace(day=30)
                        LOGGER.debug('Date has been changed for: %s' % (dateEn))
            except:
                LOGGER.debug('Could not detect calendar')

            if normalize == 'None':
                seacyc = False
            else:
                seacyc = True

            if outformat == 'ascii':
                outformat = '.txt'
            elif outformat == 'netCDF':
                outformat = '.nc'
            else:
                LOGGER.error('output format not valid')

            start = min(refSt, dateSt)
            end = max(refEn, dateEn)

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
                s,e = get_timerange(re)
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
                model_id = 'Unknown model'

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
            # Block to Detrend data
            # TODO 1 Keep trend as separate file
            # TODO 2 Think how to add options to plot abomalies AND original data...
            #        May be do archive and simulation = call.. over NOT detrended data and keep it as well
            if (dimlen > 3):
                res_tmp = get_level(regr_res, level=level)
                variable = 'z%s' % level
            else:
                res_tmp = call(resource=regr_res, spatial_wrapping='wrap')

            if detrend == 'None':
                orig_model_subset = res_tmp
            else:
                orig_model_subset = remove_mean_trend(res_tmp, varname=variable)

            # ============================

#            archive_tmp = call(resource=regr_res, time_range=[refSt, refEn], spatial_wrapping='wrap')
#            simulation_tmp = call(resource=regr_res, time_range=[dateSt, dateEn], spatial_wrapping='wrap')

            ################################
            # Prepare names for config.txt #
            ################################

            # refDatesString = dt.strftime(refSt, '%Y-%m-%d') + "_" + dt.strftime(refEn, '%Y-%m-%d')
            # simDatesString = dt.strftime(dateSt, '%Y-%m-%d') + "_" + dt.strftime(dateEn, '%Y-%m-%d')

            # Fix < 1900 issue...
            refDatesString = refSt.isoformat().strip().split("T")[0] + "_" + refEn.isoformat().strip().split("T")[0]
            simDatesString = dateSt.isoformat().strip().split("T")[0] + "_" + dateEn.isoformat().strip().split("T")[0]

            archiveNameString = "base_" + variable + "_" + refDatesString + '_%.1f_%.1f_%.1f_%.1f' \
                                % (bbox[0], bbox[2], bbox[1], bbox[3])
            simNameString = "sim_" + variable + "_" + simDatesString + '_%.1f_%.1f_%.1f_%.1f' \
                            % (bbox[0], bbox[2], bbox[1], bbox[3])

            archive = call(resource=res_tmp, time_range=[refSt, refEn], spatial_wrapping='wrap', prefix=archiveNameString)
            simulation = call(resource=res_tmp, time_range=[dateSt, dateEn], spatial_wrapping='wrap', prefix=simNameString)

            #######################################################################################

            if seacyc is True:
                seasoncyc_base, seasoncyc_sim = analogs.seacyc(archive, simulation, method=normalize)
            else:
                seasoncyc_base = None
                seasoncyc_sim = None
        except Exception as e:
            msg = 'failed to prepare archive and simulation files %s ' % e
            LOGGER.debug(msg)
            raise Exception(msg)
        ip, output = mkstemp(dir='.', suffix='.txt')
        output_file = path.abspath(output)
        files = [path.abspath(archive), path.abspath(simulation), output_file]

        LOGGER.debug("data preperation took %s seconds.", time.time() - start_time)

        ############################
        # generating the config file
        ############################

        # TODO: add MODEL name as argument

        response.update_status('writing config file', 50)
        start_time = time.time()  # measure write config ...

        try:
            config_file = analogs.get_configfile(
                files=files,
                seasoncyc_base=seasoncyc_base,
                seasoncyc_sim=seasoncyc_sim,
                base_id=model_id,
                sim_id=model_id,
                timewin=timewin,
                varname=variable,
                seacyc=seacyc,
                cycsmooth=91,
                nanalog=nanalog,
                seasonwin=seasonwin,
                distfun=distance,
                outformat=outformat,
                calccor=True,
                silent=False,
                # period=[dt.strftime(refSt, '%Y-%m-%d'), dt.strftime(refEn, '%Y-%m-%d')],
                period=[refSt.isoformat().strip().split("T")[0], refEn.isoformat().strip().split("T")[0]],
                bbox="%s,%s,%s,%s" % (bbox[0], bbox[2], bbox[1], bbox[3]))
        except Exception as e:
            msg = 'failed to generate config file %s ' % e
            LOGGER.debug(msg)
            raise Exception(msg)

        LOGGER.debug("write_config took %s seconds.", time.time() - start_time)

        ##############
        # CASTf90 call
        ##############
        import subprocess
        import shlex

        start_time = time.time()  # measure call castf90
        response.update_status('Start CASTf90 call', 60)

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
            environ['MKL_NUM_THREADS'] = str(nth)
            environ['OMP_NUM_THREADS'] = str(nth)
        except Exception as e:
            msg = 'Failed to set THREADS %s ' % e
            LOGGER.debug(msg)
        # -----------------------

        # ##### TEMPORAL WORKAROUND! With instaled hdf5-1.8.18 in anaconda ###############
        # ##### MUST be removed after castf90 recompiled with the latest hdf version
        # ##### NOT safe
        environ['HDF5_DISABLE_VERSION_CHECK'] = '1'
        # hdflib = os.path.expanduser("~") + '/anaconda/lib'
        # hdflib = os.getenv("HOME") + '/anaconda/lib'
        import pwd
        hdflib = pwd.getpwuid(getuid()).pw_dir + '/anaconda/lib'
        environ['LD_LIBRARY_PATH'] = hdflib
        # ################################################################################

        try:
            # response.update_status('execution of CASTf90', 50)
            cmd = 'analogue.out %s' % path.relpath(config_file)
            # system(cmd)
            args = shlex.split(cmd)
            output, error = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            LOGGER.info('analogue.out info:\n %s ' % output)
            LOGGER.debug('analogue.out errors:\n %s ' % error)
            response.update_status('**** CASTf90 suceeded', 70)
        except Exception as e:
            msg = 'CASTf90 failed %s ' % e
            LOGGER.error(msg)
            raise Exception(msg)

        LOGGER.debug("castf90 took %s seconds.", time.time() - start_time)

        # TODO: Add try - except for pdfs
        if plot == 'Yes':
            analogs_pdf = analogs.plot_analogs(configfile=config_file)
        else:
            analogs_pdf = 'dummy_plot.pdf'
            with open(analogs_pdf, 'a'): utime(analogs_pdf, None)

        response.update_status('preparing output', 80)

        response.outputs['analog_pdf'].file = analogs_pdf
        response.outputs['config'].file = config_file
        response.outputs['analogs'].file = output_file
        response.outputs['output_netcdf'].file = simulation
        response.outputs['target_netcdf'].file = archive

        if seacyc is True:
            response.outputs['base_netcdf'].file = seasoncyc_base
            response.outputs['sim_netcdf'].file = seasoncyc_sim
        else:
            # TODO: Still unclear how to overpass unknown number of outputs
            dummy_base = 'dummy_base.nc'
            dummy_sim = 'dummy_sim.nc'
            with open(dummy_base, 'a'): utime(dummy_base, None)
            with open(dummy_sim, 'a'): utime(dummy_sim, None)
            response.outputs['base_netcdf'].file = dummy_base
            response.outputs['sim_netcdf'].file = dummy_sim

        ########################
        # generate analog viewer
        ########################

        formated_analogs_file = analogs.reformat_analogs(output_file)
        # response.outputs['formated_analogs'].storage = FileStorage()
        response.outputs['formated_analogs'].file = formated_analogs_file
        LOGGER.info('analogs reformated')
        response.update_status('reformatted analog file', 90)

        viewer_html = analogs.render_viewer(
            # configfile=response.outputs['config'].get_url(),
            configfile=config_file,
            # datafile=response.outputs['formated_analogs'].get_url())
            datafile=formated_analogs_file)
        response.outputs['output'].file = viewer_html
        response.update_status('Successfully generated analogs viewer', 95)
        LOGGER.info('rendered pages: %s ', viewer_html)

        response.update_status('execution ended', 100)
        LOGGER.debug("total execution took %s seconds.",
                     time.time() - process_start_time)
        return response
