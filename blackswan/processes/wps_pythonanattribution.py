"""
Processes for extreme events attribution
Author: Soulivanh Thao
soulivanh.thao@lsce.ipsl.fr
Laboratoire des Sciences du Climat et de l'Environnement
"""

import sys
# from blackswan.datafetch import _PRESSUREDATA_
# from blackswan.weatherregimes import _TIMEREGIONS_
from pywps import Process
from pywps import LiteralInput
from pywps import ComplexInput, ComplexOutput
from pywps import BoundingBoxInput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata

from blackswan import config

from blackswan.log import init_process_logger

from datetime import datetime as dt
# from datetime import time as dt_time
from blackswan.pythonanattribution import analogs_generator
# from blackswan.utils import archive, archiveextract, get_calendar
from tempfile import mkstemp
from os import path, system, chdir, listdir

import logging
LOGGER = logging.getLogger("PYWPS")

DEF_OBS = path.join(config.obs_path(), 'tas_avg_NCEP_-10_30_30_60.txt')
_OBS = listdir(config.obs_path())


class PythonanattributionProcess(Process):
    def __init__(self):
        # import pdb
        # pdb.set_trace()
        inputs = [
            LiteralInput("nsim", "numbers of simulated Y to generate with analogues",
                         abstract="number of simulations",
                         default=1000,
                         data_type='integer',
                         min_occurs=0,
                         max_occurs=1,
                         ),
            ComplexInput("yfile", "file with date and value of Y",
                         abstract="file with date and value of Y",
                         min_occurs=0,
                         max_occurs=1,
                         # maxmegabites=5000,
                         supported_formats=[Format('text/plain')]
                         ),
            LiteralInput("yfile_dict", "file with date and value of Y (from local arc)",
                         abstract="file with date and value of Y (from local arc)",
                         data_type='string',
                         default=None,
                         min_occurs=0,
                         max_occurs=1,
                         # maxmegabites=5000,
                         allowed_values=['None']+_OBS
                         ),
            ComplexInput("anafile1", "Analogues result file for period P1",
                         abstract="Analogues text file computed by Analogues of Circulation processes",
                         # data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         # maxmegabites=5000,
                         supported_formats=[Format('text/plain')]
                         ),
            ComplexInput("anafile2", "Analogues result file for period P2",
                         abstract="Analogues text file computed by Analogues of Circulation processes",
                         # data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         # maxmegabites=5000,
                         supported_formats=[Format('text/plain')]
                         ),
        ]

        outputs = [
            ComplexOutput("Py_output_graphic", "Histogram of simulated Y in period P1 and P2",
                          abstract="Histograms of Y",
                          supported_formats=[Format('image/pdf')],
                          as_reference=True,
                          ),
            ComplexOutput("Ysims", "Data.Frame of simulated Y in period P1 and P2",
                          abstract="simulated Y",
                          supported_formats=[Format('text/plain')],
                          as_reference=True,
                          ),

            ComplexOutput('output_log', 'Logging information',
                          abstract="Collected logs during process run.",
                          as_reference=True,
                          supported_formats=[Format('text/plain')]
                          ),
        ]
        super(PythonanattributionProcess, self).__init__(
            self._handler,
            identifier="pythonanattribution",
            title="Attribution with analogues",
            abstract='Attributions with analogues',
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
        LOGGER.info('INPUTS %s', inputs)

    def _handler(self, request, response):
        LOGGER.debug('CURDIR XXXX : %s ' % (path.abspath(path.curdir)))
        LOGGER.debug('WORKDIR XXXX : %s ' % (self.workdir))
        chdir(self.workdir)
        LOGGER.debug('CURDIR XXXX : %s ' % (path.abspath(path.curdir)))
        init_process_logger('log.txt')

        response.update_status('execution started at : %s ' % dt.now(), 5)

        ################################
        # reading in the input arguments
        ################################
        try:
            response.update_status('execution started at : {}'.format(dt.now()), 5)

            ################################
            # reading in the input arguments
            ################################
            LOGGER.info('read in the arguments')

            nsim = request.inputs['nsim'][0].data
            LOGGER.info('nsim %s', nsim)

            if 'yfile' in request.inputs:
                yfile = request.inputs['yfile'][0].file
            elif 'yfile_dict' in request.inputs and request.inputs['yfile_dict'][0].data != 'None':
                yfile = path.join(config.obs_path(), request.inputs['yfile_dict'][0].data)
            else:
                yfile = DEF_OBS

            # yfile = request.inputs['yfile'][0].file
            # need to set default or let user to upload one
            # '/homel/nkadyg/birdhouse/var/lib/pywps/cache/blackswan/tas_avg_NCEP_-10_30_30_60.txt' 
            # ano_tas_avg_NCEP_-10_30_30_60.txt'

            # yfile = path.join(config.obs_path(), 'tas_avg_NCEP_-10_30_30_60.txt')

            LOGGER.info('yfile %s', yfile)

            anafile1 = request.inputs['anafile1'][0].file
            LOGGER.info('anafile1 %s', anafile1)

            anafile2 = request.inputs['anafile2'][0].file
            LOGGER.info('anafile2 %s', anafile2)

        except Exception as e:
            msg = 'failed to read in the arguments'
            LOGGER.exception(msg)
            raise Exception(msg)

        response.update_status('Start anattribution', 50)
        # import shlex
        # import subprocess
        import pandas

        from matplotlib import use
        use('Agg')

        import matplotlib.pyplot as plt

        from os.path import curdir, exists, join

        # try:
        ip, output_graphics = mkstemp(dir=curdir, suffix='.pdf', prefix='anna_plots')
        LOGGER.info('output_graphics %s', output_graphics)

        ip, output_txt = mkstemp(dir=curdir, suffix='.txt', prefix='anna_ysim')
        LOGGER.info('output_txt %s', output_txt)

        # compute the average of temperature in January 2018
        # ytable = pandas.read_table(yfile, sep = " ", skipinitialspace = True)
        # idx = [x >= 20180101 and x < 20190101 for x in ytable.date]
        # tas_jan18 = ytable.iloc[idx, 1]
        # tas_jan18 = tas_jan18.mean(axis=0)

        # generate other possible realisations of temperature for January 2018
        # conditionnaly to the atmospheric circulation

        # for period P1
        ysim_p1 = analogs_generator(anafile=anafile1, yfile=yfile, nsim=nsim)
        ymean_p1 = ysim_p1.mean(axis=1)
        # print ysim_p1

        # for period P2
        ysim_p2 = analogs_generator(anafile=anafile2, yfile=yfile, nsim=nsim)
        ymean_p2 = ysim_p2.mean(axis=1)
        # print ysim_p2
        LOGGER.info('analogue generator done!')

        # Format the data into a Data.Frame to plot boxplots
        plotdat = pandas.concat([ymean_p1, ymean_p2], axis=1)
        plotdat.columns = ["P1", "P2"]
        # print plotdat

        plotdat.to_csv(output_txt, sep=' ', index=False, header=True)
        LOGGER.info('writing output_txt done!')

        fig1 = plt.figure()
        # plt.axvline(x=tas_jan18)
        plt.hist(ymean_p1, alpha=0.5, label='P1')
        plt.hist(ymean_p2, alpha=0.5, label='P2')
        plt.legend(loc='upper right')
        LOGGER.info('plot1 done!')

        fig2 = plt.figure()
        plt.boxplot([ymean_p1, ymean_p2])
        LOGGER.info('plot2 done!')
        # plt.axhline(y=tas_jan18)

        from matplotlib.backends.backend_pdf import PdfPages
        pp = PdfPages(output_graphics)
        pp.savefig(fig1)
        pp.savefig(fig2)
        pp.close()
        response.update_status('**** anatribution in R suceeded', 90)
        LOGGER.info('saving plots done!')

        # except Exception as e:
        #    msg = 'Error in analogues_generator %s ' % e
        #    LOGGER.error(msg)
        #    raise Exception(msg)

        response.update_status('Anattribution done ', 92)
        ############################################
        # set the outputs
        ############################################
        response.update_status('Set the process outputs ', 95)
        response.outputs['Py_output_graphic'].file = output_graphics
        LOGGER.info('output_graphics %s', response.outputs)
        response.outputs['Ysims'].file = output_txt
        LOGGER.info('output_graphics %s', response.outputs)
        response.outputs['output_log'].file = 'log.txt'
        response.update_status('done', 100)
        return response
