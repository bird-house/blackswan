"""
Processes for event attribution with FARallnat
Author: Soulivanh Thao (sthao@lsce.ipsl.fr)
"""

# from blackswan.datafetch import _PRESSUREDATA_
# from blackswan.weatherregimes import _TIMEREGIONS_
from pywps import Process
from pywps import LiteralInput
from pywps import ComplexInput, ComplexOutput
from pywps import BoundingBoxInput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata
from blackswan.log import init_process_logger

from datetime import datetime as dt
from datetime import time as dt_time
# from blackswan import weatherregimes as wr
from blackswan.utils import archive, archiveextract, get_calendar, rename_complexinputs
from blackswan.farallnat import compute_far
from tempfile import mkstemp
from os import path, system
from os.path import curdir, exists, join, basename

import logging
LOGGER = logging.getLogger("PYWPS")


class FARallnatProcess(Process):
    def __init__(self):
        inputs = [
                
             ComplexInput('files', 'historical and rcp CMIP5 files for X and Y',
                         abstract='NetCDF Files or archive (tar/zip) containing netCDF files.',
                         metadata=[Metadata('Info')],
                         min_occurs=1,
                         max_occurs=1000,
                         supported_formats=[
                             Format('application/x-netcdf'),
                             Format('application/x-tar'),
                             Format('application/zip'),
                         ]),
    
            LiteralInput("xname", "name of the variable X",
                         abstract = "name of the variable X?",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         ),
                         
            LiteralInput("yname", "name of the variable Y",
                         abstract = "name of the variable Y",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         ),
                         
            LiteralInput("rcp", "RCP scenario to use",
                         abstract = "name of RCP scenario to use",
                         data_type='string',
                         default =  "rcp85",
                         min_occurs=1,
                         max_occurs=1,
                         ),
               
           LiteralInput("y_compute_ano", "anomalies for Y?",
                         abstract = "Should we remove the mean seasonal cycle from Y?",
                         default = True,
                         data_type='boolean',
                         min_occurs=1,
                         max_occurs=1,
                         ),
            
            LiteralInput("y_start_ano", "Anomalie starting date for Y",
                         abstract="Starting date of the period use to define the anomalies for Y",
                         default="1850-01-01T00:00:00",
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput("y_end_ano", "Anomalie endinn date for Y",
                         abstract="Ending date of the period use to define the anomalies for Y",
                         default="2100-12-31T23:59:59",
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('y_bbox', 'Y Bounding Box',
                         data_type='string',
                         abstract="Enter a bbox: min_lon, max_lon, min_lat, max_lat."
                            " min_lon=Western longitude,"
                            " max_lon=Eastern longitude,"
                            " min_lat=Southern or northern latitude,"
                            " max_lat=Northern or southern latitude."
                            " For example: -80,50,20,70",
                         min_occurs=1,
                         max_occurs=1,
                         default='-180,180,-90,90',
                         ),
            
            LiteralInput("y_season", "subperiod of year  for Y",
                         abstract="Select the months to define a subperiod of a year",
                         default="JFMAMJJASOND",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         ),

            LiteralInput("y_first_spatial", "first spatial aggregation for Y?",
                         abstract = "Should we first aggregate data spatially and them temporaly for Y?"
                         "if True: first spatial and then temporal aggregation."
                         "if False: first temporal and then spatial aggregation",
                         default = True,
                         data_type='boolean',
                         min_occurs=1,
                         max_occurs=1,
                         ),
                         
            LiteralInput("y_spatial_aggregator", "Y spatial aggregating function",
                    abstract="How to aggregate data spatially:"
                    "mean, min or max available",
                         default="mean",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=["mean", "min", "max"]
                         ),

            LiteralInput("y_time_aggregator", "Y time aggregating function",
                    abstract="How to aggregate data temporally:"
                    "mean, min or max available",
                         default="mean",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=["mean", "min", "max"]
                         ),

            LiteralInput("x_compute_ano", "anomalies for X?",
                         abstract = "Should we remove the mean seasonal cycle from X?",
                         default = True,
                         data_type='boolean',
                         min_occurs=1,
                         max_occurs=1,
                         ),
            
            LiteralInput("x_start_ano", "Anomalie starting date for X",
                         abstract="Starting date of the period use to define the anomalies for X",
                         default="1850-01-01T00:00:00",
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput("x_end_ano", "Anomalie endinn date for X",
                         abstract="Ending date of the period use to define the anomalies for X",
                         default="2100-12-31T23:59:59",
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         ),

            LiteralInput('x_bbox', 'X Bounding Box',
                         data_type='string',
                         abstract="Enter a bbox: min_lon, max_lon, min_lat, max_lat."
                            " min_lon=Western longitude,"
                            " max_lon=Eastern longitude,"
                            " min_lat=Southern or northern latitude,"
                            " max_lat=Northern or southern latitude."
                            " For example: -80,50,20,70",
                         min_occurs=1,
                         max_occurs=1,
                         default='-180,180,-90,90',
                         ),
            
            LiteralInput("x_season", "subperiod of year for Y",
                         abstract="Select the months to define a subperiod of a year",
                         default="JFMAMJJASOND",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         ),

            LiteralInput("x_first_spatial", "first spatial aggregation for X",
                         abstract = "Should we first aggregate data spatially and them temporaly for X?"
                         "if True: first spatial and then temporal aggregation."
                         "if False: first temporal and then spatial aggregation",
                         default = True,
                         data_type='boolean',
                         min_occurs=1,
                         max_occurs=1,
                         ),
                         
            LiteralInput("x_spatial_aggregator", "X spatial aggregating function",
                    abstract="How to aggregate data spatially:"
                    "mean, min or max available",
                         default="mean",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=["mean", "min", "max"]
                         ),

            LiteralInput("x_time_aggregator", "Y time aggregating function",
                    abstract="How to aggregate data temporally:"
                    "mean, min or max available",
                         default="mean",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=["mean", "min", "max"]
                         ),

            LiteralInput("stat_model", "Statistical model",
                         abstract="Statistical model used to estimate the FAR"
                         "available: gauss_fit, gev_fit or gpd_fit",
                         default='gauss_fit',
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=["gauss_fit", "gev_fit", "gpd_fit"]
                         ),

            LiteralInput("qthreshold", "GPD threshold ",
                         abstract="if gpd_fit is chosen, qthreshold is the quantile level that is used to define the GPD threshold",
                         default='0.9',
                         data_type='float',
                         min_occurs=0,
                         max_occurs=1,
                         ),
                         
            LiteralInput("nbootstrap", "number of bootstrap samples ",
                         abstract="number of bootstrap samples used to compute the confidence intervals",
                         default= 250,
                         data_type='integer',
                         min_occurs=1,
                         max_occurs=1,
                         ),
        ]

        outputs = [
            ComplexOutput("Routput_graphics", "FAR graphs",
                          abstract="graphs for the FAR",
                          supported_formats=[Format('image/pdf')],
                          as_reference=True,
                          ),

            ComplexOutput("Rdata", "R - rds file",
                          abstract="R data used for the FAR analysis",
                          supported_formats=[Format('text/plain')],
                          as_reference=True,
                          ),

            ComplexOutput('output_log', 'Logging information',
                          abstract="Collected logs during process run.",
                          as_reference=True,
                          supported_formats=[Format('text/plain')]
                          ),
        ]
        super(FARallnatProcess, self).__init__(
            self._handler,
            identifier="FARallnat",
            title="FAR analysis using CMIP5 data",
            abstract='non-stationnary FAR statistical analysis using CMIP5 data',
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

        response.update_status('execution started at : %s ' % dt.now(), 10)

        ################################
        # reading in the input arguments
        ################################
        try:
            response.update_status('execution started at : {}'.format(dt.now()), 20)

            ################################
            # reading in the input arguments
            ################################
            LOGGER.debug('read in the arguments')
            LOGGER.debug('request.input[files] %s ' % (request.inputs['files']))
            # files = archiveextract(resource=[res.file for res in request.inputs['files']])
            files = [res.file for res in request.inputs['files']]
            # files = request.inputs['files'][0].data.split(",")
            LOGGER.debug('files %s ' % (files))
            # files = archiveextract(resource=files)
            LOGGER.debug('files %s ' % (files))
            
            xname = request.inputs['xname'][0].data
            LOGGER.debug('xname: %s ' % (xname))
            yname = request.inputs['yname'][0].data
            LOGGER.debug('yname: %s ' % (yname))
            
            rcp = request.inputs['rcp'][0].data.lower()
            LOGGER.debug('rcp: %s ' % (rcp))

            xfiles = [f for f in files if basename(f).split("_")[0] == xname]
            f_xhist = [f for f in xfiles if basename(f).split("_")[3] == "historical"]
            f_xrcp = [f for f in xfiles if basename(f).split("_")[3] in rcp]
            LOGGER.debug('xfiles %s ' % (xfiles))
            LOGGER.debug('f_xhist %s ' % (f_xhist))
            LOGGER.debug('f_xrcp %s ' % (f_xrcp))

            yfiles = [f for f in files if basename(f).split("_")[0] == yname]
            f_yhist = [f for f in yfiles if basename(f).split("_")[3] == "historical"]
            f_yrcp = [f for f in yfiles if basename(f).split("_")[3] in rcp]
            LOGGER.debug('yfiles %s ' % (yfiles))
            LOGGER.debug('f_yhist %s ' % (f_yhist))
            LOGGER.debug('f_yrcp %s ' % (f_yrcp))
            
            y_compute_ano = request.inputs['y_compute_ano'][0].data
            x_compute_ano = request.inputs['x_compute_ano'][0].data
            LOGGER.debug('Y compute_ano: %s ' % (y_compute_ano))
            LOGGER.debug('X compute_ano: %s ' % (x_compute_ano))
 
            y_start_ano = request.inputs['y_start_ano'][0].data
            y_end_ano = request.inputs['y_end_ano'][0].data
            x_start_ano = request.inputs['x_start_ano'][0].data
            x_end_ano = request.inputs['x_end_ano'][0].data
            LOGGER.debug('Y start_ano: %s ' % (y_start_ano))
            LOGGER.debug('Y end_ano: %s ' % (y_end_ano))
            LOGGER.debug('X start_ano: %s ' % (x_start_ano))
            LOGGER.debug('X end_ano: %s ' % (x_end_ano))

            y_bbox = request.inputs['y_bbox'][0].data
            x_bbox = request.inputs['x_bbox'][0].data
            LOGGER.debug('Y bbox: %s ' % (y_bbox))
            LOGGER.debug('X bbox: %s ' % (x_bbox))

            y_season = request.inputs['y_season'][0].data
            x_season = request.inputs['x_season'][0].data
            LOGGER.debug('Y season: %s ' % (y_season))
            LOGGER.debug('X season: %s ' % (x_season))

            y_spatial_aggregator = request.inputs['y_spatial_aggregator'][0].data
            x_spatial_aggregator = request.inputs['x_spatial_aggregator'][0].data
            LOGGER.debug('Y spatial_aggregator: %s ' % (y_spatial_aggregator))
            LOGGER.debug('X spatial_aggregator: %s ' % (x_spatial_aggregator))

            y_time_aggregator = request.inputs['y_time_aggregator'][0].data
            x_time_aggregator = request.inputs['x_time_aggregator'][0].data
            LOGGER.debug('Y time_aggregator: %s ' % (y_time_aggregator))
            LOGGER.debug('X time_aggregator: %s ' % (x_time_aggregator))
            
            y_first_spatial = request.inputs['y_first_spatial'][0].data
            x_first_spatial = request.inputs['x_first_spatial'][0].data
            LOGGER.debug('Y first_spatial: %s ' % (y_first_spatial))
            LOGGER.debug('X first_spatial: %s ' % (x_first_spatial))

            stat_model = request.inputs['stat_model'][0].data
            LOGGER.debug('stat_model: %s ' % (stat_model))

            qthreshold = request.inputs['qthreshold'][0].data
            LOGGER.debug('qthreshold: %s ' % (qthreshold))
            
            nbootstrap = request.inputs['nbootstrap'][0].data
            LOGGER.debug('nbootstrap: %s ' % (nbootstrap))

        except Exception as e:
            msg = 'failed to read in the arguments %s' % e
            LOGGER.exception(msg)
            raise Exception(msg)

        ####################
        # Run farallnar.py
        ####################
        response.update_status('Start FARallnat analysis', 5)
        try:
            # ip, Routput_graphics = mkstemp(dir=curdir, suffix='.pdf')
            # ip, Rdata = mkstemp(dir=curdir, suffix='.rds')
            Rdata = "FARallnat_data.rds"
            Routput_graphics = "FARallnat_plots.pdf"
            # add xname and yname as arguments
            compute_far(plot_pdf = Routput_graphics,
                    data_rds = Rdata,
                    yvarname = yname,
                    xvarname = xname,
                    f_yhist = f_yhist,
                    f_yrcp =  f_yrcp,
                    f_xhist =  f_xhist,
                    f_xrcp = f_xrcp,
                    y_compute_ano = y_compute_ano,
                    y_start_ano = y_start_ano,
                    y_end_ano = y_end_ano,
                    y_bbox = y_bbox,
                    y_season = y_season,
                    y_first_spatial = y_first_spatial,
                    y_spatial_aggregator = y_spatial_aggregator,
                    y_time_aggregator = y_time_aggregator,
                    x_compute_ano = x_compute_ano,
                    x_start_ano = x_start_ano,
                    x_end_ano = x_end_ano,
                    x_bbox = x_bbox,
                    x_season = x_season,
                    x_first_spatial = x_first_spatial,
                    x_spatial_aggregator = x_spatial_aggregator,
                    x_time_aggregator = x_time_aggregator,
                    stat_model = stat_model,
                    qthreshold = qthreshold,
                    nbootstrap = nbootstrap
                    )
 
        except Exception as e:
            msg = 'fails in farallnat.py %s' % e
            LOGGER.error(msg)
            raise Exception(msg)

        response.update_status('FARallanat analysis done ', 90)
        
        ############################################
        # set the outputs
        ############################################
        # response.update_status('Set the process outputs ', 95)
        response.outputs['Routput_graphics'].file = Routput_graphics
        response.outputs['Rdata'].file = Rdata
        response.update_status('done', 100)
        return response
