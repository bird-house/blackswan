import os
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
from pywps import LiteralInput, LiteralOutput
from pywps import ComplexInput, ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata
from pywps.inout.storage import FileStorage

from blackswan.datafetch import _PRESSUREDATA_
from blackswan.datafetch import reanalyses as rl
from blackswan.ocgis_module import call
from blackswan import analogs
from blackswan.utils import rename_complexinputs
from blackswan.utils import get_variable, rename_variable
from blackswan.utils import get_files_size
from blackswan.calculation import remove_mean_trend
from blackswan.log import init_process_logger

import logging
LOGGER = logging.getLogger("PYWPS")


class AnalogsRe2ReProcess(Process):
    def __init__(self):
        inputs = [

            LiteralInput("reanalyses", "Reanalyses Data",
                         abstract="Choose a reanalyses dataset as simulation",
                         default="NCEP_slp",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=['NCEP_slp', 'NCEP_z1000', 'NCEP_z850',
                                         'NCEP_z700', 'NCEP_z600', 'NCEP_z500', 'NCEP_z400',
                                         'NCEP_z300', 'NCEP_z250', 'NCEP_z200', 'NCEP_z150',
                                         'NCEP_z100', 'NCEP_z70', 'NCEP_z50', 'NCEP_z30',
                                         'NCEP_z20', 'NCEP_z10']
                         ),

            LiteralInput("Refreanalyses", "Reanalyses Data",
                         abstract="Choose a reanalyses dataset where look for analogs",
                         default="20CRV2c_prmsl",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=['20CRV2c_prmsl', '20CRV2c_z1000', '20CRV2c_z850',
                                         '20CRV2c_z700', '20CRV2c_z600', '20CRV2c_z500',
                                         '20CRV2c_z400', '20CRV2c_z300', '20CRV2c_z250',
                                         '20CRV2c_z200', '20CRV2c_z150', '20CRV2c_z100',
                                         '20CRV2c_z70', '20CRV2c_z50', '20CRV2c_z30',
                                         '20CRV2c_z20', '20CRV2c_z10']
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

            LiteralInput('dateSt', 'Start date of analysis period',
                         data_type='date',
                         abstract='First day of the period to be analysed',
                         default='2018-03-01',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('dateEn', 'End date of analysis period',
                         data_type='date',
                         abstract='Last day of the period to be analysed',
                         default='2018-03-05',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('refSt', 'Start date of reference period',
                         data_type='date',
                         abstract='First day of the period where analogues being picked',
                         default='1900-01-01',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('refEn', 'End date of reference period',
                         data_type='date',
                         abstract='Last day of the period where analogues being picked',
                         default='2014-12-31',
                         min_occurs=0,
                         max_occurs=1,
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
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
                          ),

            ComplexOutput("formated_analogs", "Formated Analogues File",
                          abstract="Formated analogues file for viewer",
                          supported_formats=[Format("text/plain")],
                          as_reference=True,
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

        super(AnalogsRe2ReProcess, self).__init__(
            self._handler,
            identifier="analogs_re2re",
            title="Analogues of circulation (based on 2 reanalyses datasets)",
            abstract='Search for days with analogue pressure pattern for NCEP in 20CRV2c reanalyses data sets',
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

        try:
            response.update_status('read input parameter : %s ' % dt.now(), 6)

            refSt = request.inputs['refSt'][0].data
            refEn = request.inputs['refEn'][0].data
            dateSt = request.inputs['dateSt'][0].data
            dateEn = request.inputs['dateEn'][0].data

            seasonwin = request.inputs['seasonwin'][0].data
            nanalog = request.inputs['nanalog'][0].data

            bboxDef = '-20,40,30,70' # in general format

            bbox = []
            bboxStr = request.inputs['BBox'][0].data
            LOGGER.debug('BBOX selected by user: %s ' % (bboxStr))
            bboxStr = bboxStr.split(',')

            # Checking for wrong cordinates and apply default if nesessary
            if (abs(float(bboxStr[0])) > 180 or
                    abs(float(bboxStr[1]) > 180) or
                    abs(float(bboxStr[2]) > 90) or
                    abs(float(bboxStr[3])) > 90):
                bboxStr = bboxDef # request.inputs['BBox'].default  # .default doesn't work anymore!!!
                LOGGER.debug('BBOX is out of the range, using default instead: %s ' % (bboxStr))
                bboxStr = bboxStr.split(',')

            bbox.append(float(bboxStr[0]))
            bbox.append(float(bboxStr[2]))
            bbox.append(float(bboxStr[1]))
            bbox.append(float(bboxStr[3]))
            LOGGER.debug('BBOX for ocgis: %s ' % (bbox))
            LOGGER.debug('BBOX original: %s ' % (bboxStr))

            plot = request.inputs['plot'][0].data
            distance = request.inputs['dist'][0].data
            outformat = request.inputs['outformat'][0].data
            timewin = request.inputs['timewin'][0].data

            model_var = request.inputs['reanalyses'][0].data
            model, var = model_var.split('_')

            ref_model_var = request.inputs['Refreanalyses'][0].data
            ref_model, ref_var = ref_model_var.split('_')

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
            LOGGER.debug('date: %s %s %s %s ' % (type(refSt), refEn, dateSt, dateSt))

            #normalize == 'None':
            seacyc = False

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

        getlevel = False
        if 'z' in var:
            level = var.strip('z')
        else:
            level = None

        ##########################################
        # fetch Data from original data archive
        ##########################################
                
        try:
            model_nc = rl(start=dateSt.year, end=dateEn.year,
                          dataset=model, variable=var,
                          getlevel=getlevel)

            ref_model_nc = rl(start=refSt.year, end=refEn.year,
                              dataset=ref_model, variable=ref_var,
                              getlevel=getlevel)

            LOGGER.info('reanalyses data fetched')
        except Exception:
            msg = 'failed to get reanalyses data'
            LOGGER.exception(msg)
            raise Exception(msg)

        response.update_status('subsetting region of interest', 10)

        # Checking memory and dataset size
        model_size = get_files_size(model_nc)
        ref_model_size = get_files_size(ref_model_nc)

        m_size = max(model_size, ref_model_size)

        memory_avail = psutil.virtual_memory().available
        thrs = 0.2 # 10%

        if (m_size >= thrs * memory_avail):
            ser_r = True
        else:
            ser_r = False

        LOGGER.debug('Available Memory: %s ' % (memory_avail))
        LOGGER.debug('Dataset size: %s ' % (m_size))
        LOGGER.debug('Threshold: %s ' % (thrs*memory_avail))
        LOGGER.debug('Serial or at once: %s ' % (ser_r))

        # #####################################################
        # Construct descriptive filenames for the three files #
        # listed in config file                               #
        # TODO check strftime for years <1900 (!)             #
        # #####################################################

        refDatesString = dt.strftime(refSt, '%Y-%m-%d') + "_" + dt.strftime(refEn, '%Y-%m-%d')
        simDatesString = dt.strftime(dateSt, '%Y-%m-%d') + "_" + dt.strftime(dateEn, '%Y-%m-%d')

        archiveNameString = "base_" + var + "_" + refDatesString + '_%.1f_%.1f_%.1f_%.1f' \
                            % (bbox[0], bbox[2], bbox[1], bbox[3])
        simNameString = "sim_" + var + "_" + simDatesString + '_%.1f_%.1f_%.1f_%.1f' \
                            % (bbox[0], bbox[2], bbox[1], bbox[3])

        if ('z' in var):  
            # ------------------ NCEP -------------------
            tmp_total = []
            origvar = get_variable(model_nc)

            for z in model_nc:
                b0=call(resource=z, variable=origvar, level_range=[int(level), int(level)], geom=bbox,
                spatial_wrapping='wrap', prefix='levdom_'+os.path.basename(z)[0:-3])
                tmp_total.append(b0)

            time_range=[dateSt, dateEn]

            tmp_total = sorted(tmp_total, key=lambda i: os.path.splitext(os.path.basename(i))[0])
            inter_subset_tmp = call(resource=tmp_total, variable=origvar, time_range=time_range)

            # Clean
            for i in tmp_total:
                tbr='rm -f %s' % (i) 
                os.system(tbr)  

            # Create new variable
            ds = Dataset(inter_subset_tmp, mode='a')
            z_var = ds.variables.pop(origvar)
            dims = z_var.dimensions
            new_var = ds.createVariable('z%s' % level, z_var.dtype, dimensions=(dims[0], dims[2], dims[3]))
            new_var[:, :, :] = squeeze(z_var[:, 0, :, :])
            ds.close()
            simulation = call(inter_subset_tmp, variable='z%s' % level, prefix=simNameString)

            # ------------------ 20CRV2c -------------------
            tmp_total = []
            origvar = get_variable(ref_model_nc)

            for z in ref_model_nc:

                tmp_n='tmp_%s' % (uuid.uuid1())
                # select level and regrid
                b0=call(resource=z, variable=origvar, level_range=[int(level), int(level)],
                        spatial_wrapping='wrap', cdover='system',
                        regrid_destination=model_nc[0], regrid_options='bil', prefix=tmp_n)

                # select domain
                b01=call(resource=b0, variable=origvar, geom=bbox, spatial_wrapping='wrap', prefix='levregr_'+os.path.basename(z)[0:-3])
                tbr='rm -f %s' % (b0)
                os.system(tbr)
                tbr='rm -f %s.nc' % (tmp_n)
                os.system(tbr)

                tmp_total.append(b01)

            time_range=[refSt, refEn]

            tmp_total = sorted(tmp_total, key=lambda i: os.path.splitext(os.path.basename(i))[0])
            ref_inter_subset_tmp = call(resource=tmp_total, variable=origvar, time_range=time_range)

            # Clean
            for i in tmp_total:
                tbr='rm -f %s' % (i) 
                os.system(tbr)  

            # Create new variable
            ds = Dataset(ref_inter_subset_tmp, mode='a')
            z_var = ds.variables.pop(origvar)
            dims = z_var.dimensions
            new_var = ds.createVariable('z%s' % level, z_var.dtype, dimensions=(dims[0], dims[2], dims[3]))
            new_var[:, :, :] = squeeze(z_var[:, 0, :, :])
            ds.close()
            archive = call(ref_inter_subset_tmp, variable='z%s' % level, prefix=archiveNameString)

        else:
            if ser_r:
                LOGGER.debug('Process reanalysis step-by-step')
                # ----- NCEP ------
                tmp_total = []
                for z in model_nc:
                    b0=call(resource=z, variable=var, geom=bbox, spatial_wrapping='wrap',
                            prefix='Rdom_'+os.path.basename(z)[0:-3])
                    tmp_total.append(b0)

                tmp_total = sorted(tmp_total, key=lambda i: os.path.splitext(os.path.basename(i))[0])
                simulation = call(resource=tmp_total, variable=var, time_range=[dateSt, dateEn], prefix=simNameString)

                # Clean
                for i in tmp_total:
                    tbr='rm -f %s' % (i) 
                    os.system(tbr) 

                # ----- 20CRV2c ------
                tmp_n='tmp_%s' % (uuid.uuid1())
                tmp_total = []
                for z in ref_model_nc:
                    # regrid
                    b0=call(resource=z, variable=ref_var, spatial_wrapping='wrap', cdover='system',
                            regrid_destination=model_nc[0], regrid_options='bil', prefix=tmp_n)
                    # select domain
                    b01=call(resource=b0, variable=ref_var, geom=bbox, spatial_wrapping='wrap',
                             prefix='ref_Rdom_'+os.path.basename(z)[0:-3])

                    tbr='rm -f %s' % (b0)
                    os.system(tbr)
                    tbr='rm -f %s.nc' % (tmp_n)
                    os.system(tbr)

                    tmp_total.append(b01)

                tmp_total = sorted(tmp_total, key=lambda i: os.path.splitext(os.path.basename(i))[0])
                archive = call(resource=tmp_total, variable=ref_var, time_range=[refSt, refEn], prefix=archiveNameString)
                # Clean
                for i in tmp_total:
                    tbr='rm -f %s' % (i) 
                    os.system(tbr)
            else:
                LOGGER.debug('Using whole dataset at once')

                simulation = call(resource=model_nc, variable=var,
                                        geom=bbox, spatial_wrapping='wrap', time_range=[dateSt, dateEn], prefix=simNameString)

                ref_inter_subset_tmp = call(resource=ref_model_nc, variable=ref_var, spatial_wrapping='wrap',
                                            cdover='system', regrid_destination=model_nc[0], regrid_options='bil')

                archive = call(resource=ref_inter_subset_tmp, geom=bbox, spatial_wrapping='wrap', time_range=[refSt, refEn], prefix=archiveNameString)

        response.update_status('datasets subsetted', 15)

        LOGGER.debug("get_input_subset_dataset took %s seconds.",
                     time.time() - start_time)
        response.update_status('**** Input data fetched', 20)

        ########################
        # input data preperation
        ########################
        response.update_status('Start preparing input data', 30)
        start_time = time.time()  # measure data preperation ...

        LOGGER.info('archive and simulation files generated: %s, %s'
                    % (archive, simulation))

        # Rename variable (TODO: For this specific process we know names: slp and prmsl...)
        try:
            if level is not None:
                out_var = 'z%s' % level
            else:
                var_archive = get_variable(archive)
                var_simulation = get_variable(simulation)
                if var_archive != var_simulation:
                    rename_variable(archive, oldname=var_archive, newname=var_simulation)
                    out_var = var_simulation
                    LOGGER.info('varname %s in netCDF renamed to %s' % (var_archive, var_simulation))
        except:
            msg = 'failed to rename variable in target files'
            LOGGER.exception(msg)
            raise Exception(msg)


        #seacyc is False:
        seasoncyc_base = seasoncyc_sim = None

        output_file = 'output.txt'
        files = [os.path.abspath(archive), os.path.abspath(simulation), output_file]
        LOGGER.debug("Data preperation took %s seconds.",
                     time.time() - start_time)

        ############################
        # generate the config file
        ############################
        config_file = analogs.get_configfile(
            files=files,
            seasoncyc_base=seasoncyc_base,
            seasoncyc_sim=seasoncyc_sim,
            base_id=ref_model,
            sim_id=model,
            timewin=timewin,
            varname=out_var,
            seacyc=seacyc,
            cycsmooth=91,
            nanalog=nanalog,
            seasonwin=seasonwin,
            distfun=distance,
            outformat=outformat,
            calccor=True,
            silent=False,
            period=[dt.strftime(refSt, '%Y-%m-%d'), dt.strftime(refEn, '%Y-%m-%d')],
            bbox="{0[0]},{0[2]},{0[1]},{0[3]}".format(bbox))
        response.update_status('generated config file', 40)
        #######################
        # CASTf90 call
        #######################
        start_time = time.time()  # measure call castf90

        #-----------------------
        try:
            import ctypes
            # TODO: This lib is for linux
            mkl_rt = ctypes.CDLL('libmkl_rt.so')
            nth=mkl_rt.mkl_get_max_threads()
            LOGGER.debug('Current number of threads: %s' % (nth))
            mkl_rt.mkl_set_num_threads(ctypes.byref(ctypes.c_int(64)))
            nth=mkl_rt.mkl_get_max_threads()
            LOGGER.debug('NEW number of threads: %s' % (nth))
            # TODO: Does it \/\/\/ work with default shell=False in subprocess... (?)
            os.environ['MKL_NUM_THREADS']=str(nth)
            os.environ['OMP_NUM_THREADS']=str(nth)
        except Exception as e:
            msg = 'Failed to set THREADS %s ' % e
            LOGGER.debug(msg)
        #-----------------------

        # ##### TEMPORAL WORKAROUND! With instaled hdf5-1.8.18 in anaconda ###############
        # ##### MUST be removed after castf90 recompiled with the latest hdf version
        # ##### NOT safe
        os.environ['HDF5_DISABLE_VERSION_CHECK'] = '1'
        #hdflib = os.path.expanduser("~") + '/anaconda/lib'
        #hdflib = os.getenv("HOME") + '/anaconda/lib'
        import pwd
        hdflib = pwd.getpwuid(os.getuid()).pw_dir + '/anaconda/lib'
        os.environ['LD_LIBRARY_PATH'] = hdflib
        # ################################################################################

        response.update_status('Start CASTf90 call', 50)
        try:
            # response.update_status('execution of CASTf90', 50)
            cmd = ['analogue.out', config_file]
            LOGGER.debug("castf90 command: %s", cmd)
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            LOGGER.info('analogue output:\n %s', output)
            response.update_status('**** CASTf90 suceeded', 60)
        except CalledProcessError as e:
            msg = 'CASTf90 failed:\n{0}'.format(e.output)
            LOGGER.exception(msg)
            raise Exception(msg)
        LOGGER.debug("castf90 took %s seconds.", time.time() - start_time)

        # TODO: Add try - except for pdfs
        if plot == 'Yes':
            analogs_pdf = analogs.plot_analogs(configfile=config_file)   
        else:
            analogs_pdf = 'dummy_plot.pdf'
            with open(analogs_pdf, 'a'): os.utime(analogs_pdf, None)

        response.update_status('preparing output', 70)

        response.outputs['analog_pdf'].file = analogs_pdf 
        response.outputs['config'].file = config_file
        response.outputs['analogs'].file = output_file

        ########################
        # generate analog viewer
        ########################

        formated_analogs_file = analogs.reformat_analogs(output_file)
        # response.outputs['formated_analogs'].storage = FileStorage()
        response.outputs['formated_analogs'].file = formated_analogs_file
        LOGGER.info('analogs reformated')
        response.update_status('reformatted analog file', 80)

        viewer_html = analogs.render_viewer(
            # configfile=response.outputs['config'].get_url(),
            configfile=config_file,
            # datafile=response.outputs['formated_analogs'].get_url())
            datafile=formated_analogs_file)
        response.outputs['output'].file = viewer_html
        response.update_status('Successfully generated analogs viewer', 90)
        LOGGER.info('rendered pages: %s ', viewer_html)

        response.update_status('execution ended', 100)
        LOGGER.debug("total execution took %s seconds.",
                     time.time() - process_start_time)
        return response





