"""
Processes for Species distribution
Author: Nils Hempelmann (nils.hempelmann@lsce.ipsl.fr)
"""

import tempfile

from flyingpigeon.sdm import _SDMINDICES_
from flyingpigeon.log import init_process_logger

from os.path import basename
from flyingpigeon import sdm
from flyingpigeon.utils import download
from flyingpigeon.utils import archive, archiveextract  # , get_domain
from flyingpigeon.utils import rename_complexinputs
from flyingpigeon.visualisation import map_PAmask
from flyingpigeon.visualisation import map_gbifoccurrences

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexInput, ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata

import logging
LOGGER = logging.getLogger("PYWPS")


class SDMcsvindicesProcess(Process):
    def __init__(self):
        inputs = [

            ComplexInput("resources", "Precalculated Indices",
                         abstract="Precalculated Indices as basis for the SDM calculation"
                                  " (list of netCDF files or tar/zip archive)",
                         min_occurs=1,
                         max_occurs=500,
                         supported_formats=[
                            Format('application/x-netcdf'),
                            Format('application/x-tar'),
                            Format('application/zip')],
                         ),

            LiteralInput("gbif", "GBIF csv file",
                         abstract="GBIF table (csv) with tree occurence \
                         (output of 'GBIF data fetch' process )",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                        #  default='https://bovec.dkrz.de/download/wpsoutputs/flyingpigeon/392f1c34-b4d1-11e7-a589-109836a7cf3a/tmp95yvix.csv'
                         ),

            LiteralInput("period", "Reference period",
                         abstract="Reference period for climate conditions\
                         (all = entire timeseries)",
                         default="all",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=['all', '1951-1980', '1961-1990',
                                         '1971-2000', '1981-2010']
                         ),

            LiteralInput("archive_format", "Archive format",
                         abstract="Result files will be compressed into archives.\
                                  Choose an appropriate format",
                         default="tar",
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=['zip', 'tar']
                         )
        ]

        outputs = [
            ComplexOutput("output_gbif", "Graphic of GBIF coordinates",
                          abstract="PNG graphic file showing the presence of tree species\
                                    according to the CSV file",
                          supported_formats=[Format('image/png')],
                          as_reference=True,
                          ),

            ComplexOutput("output_PA", "Graphic of PA mask",
                          abstract="PNG graphic file showing PA mask generated based on\
                                    netCDF spatial increment",
                          supported_formats=[Format('image/png')],
                          as_reference=True,
                          ),

            ComplexOutput("output_reference", "Climate indices for growth conditions of reference period",
                          abstract="Archive (tar/zip) containing calculated climate indices",
                          supported_formats=[Format('application/x-tar'),
                                             Format('application/zip')
                                             ],
                          as_reference=True,
                          ),

            ComplexOutput("output_prediction", "predicted growth conditions",
                          abstract="Archive containing files of the predicted\
                                     growth conditions",
                          supported_formats=[Format('application/x-tar'),
                                             Format('application/zip')
                                             ],
                          as_reference=True,
                          ),

            ComplexOutput("output_info", "GAM statistics information",
                          abstract="Graphics and information of the learning statistics",
                          supported_formats=[Format("application/pdf")],
                          as_reference=True,
                          ),
            ComplexOutput('output_log', 'Logging information',
                          abstract="Collected logs during process run.",
                          as_reference=True,
                          supported_formats=[Format('text/plain')]
                          )
        ]

        super(SDMcsvindicesProcess, self).__init__(
            self._handler,
            identifier="sdm_csvindices",
            title="Species distribution Model (GBIF-CSV Table and Indices as calculation basis)",
            version="0.10",
            metadata=[
                Metadata("LWF", "http://www.lwf.bayern.de/"),
                Metadata(
                    "Doc",
                    "http://flyingpigeon.readthedocs.io/en/latest/descriptions/index.html#species-distribution-model"),
                Metadata("paper",
                         "http://www.hindawi.com/journals/jcli/2013/787250/"),
                Metadata("Tutorial",
                         "http://flyingpigeon.readthedocs.io/en/latest/tutorials/sdm.html"),
            ],
            abstract="Indices preparation for SDM process",
            inputs=inputs,
            outputs=outputs,
            status_supported=True,
            store_supported=True,
        )

    def _handler(self, request, response):

        init_process_logger('log.txt')
        response.outputs['output_log'].file = 'log.txt'

        response.update_status('Start process', 0)

        try:
            response.update_status('reading the arguments', 5)
            resources = archiveextract(
                resource=rename_complexinputs(request.inputs['resources']))
            period = request.inputs['period']
            period = period[0].data
            archive_format = request.inputs['archive_format'][0].data
            LOGGER.info("all arguments read in nr of files in resources: %s" % len(resources))
        except:
            LOGGER.exception('failed to read in the arguments')

        try:
            gbif_url = request.inputs['gbif'][0].data
            csv_file = download(gbif_url)
            LOGGER.info('CSV file fetched sucessfully: %s' % csv_file)
        except:
            LOGGER.exception('failed to fetch GBIF file')

        try:
            response.update_status('read in latlon coordinates', 10)
            latlon = sdm.latlon_gbifcsv(csv_file)
            LOGGER.info('read in the latlon coordinates')
        except:
            LOGGER.exception('failed to extract the latlon points')

        try:
            response.update_status('plot map', 20)
            occurence_map = map_gbifoccurrences(latlon)
            LOGGER.info('GBIF occourence ploted')
        except:
            LOGGER.exception('failed to plot occurence map')

        try:
            # sort indices
            indices_dic = sdm.sort_indices(resources)
            LOGGER.info('indice files sorted in dictionary')
        except:
            msg = 'failed to sort indices'
            LOGGER.exception(msg)
            indices_dic = {'dummy': []}

        ncs_references = []
        species_files = []
        stat_infos = []
        PAmask_pngs = []

        response.update_status('Start processing for %s Datasets' % len(indices_dic.keys()))
        for count, key in enumerate(indices_dic.keys()):
            try:
                staus_nr = 40 + count * 10
                response.update_status('Start processing of %s' % key, staus_nr)
                ncs = indices_dic[key]
                LOGGER.info('with %s files' % len(ncs))

                try:
                    response.update_status('generating the PA mask', 20)
                    PAmask = sdm.get_PAmask(coordinates=latlon, nc=ncs[0])
                    LOGGER.info('PA mask sucessfully generated')
                except:
                    LOGGER.exception('failed to generate the PA mask')

                try:
                    response.update_status('Ploting PA mask', 25)
                    PAmask_pngs.extend([map_PAmask(PAmask)])
                except:
                    LOGGER.exception('failed to plot the PA mask')

                try:
                    ncs_reference = sdm.get_reference(ncs_indices=ncs, period=period)
                    ncs_references.extend(ncs_reference)
                    LOGGER.info('reference indice calculated %s '
                                % ncs_references)
                    response.update_status('reference indice calculated', staus_nr + 2)
                except:
                    msg = 'failed to calculate the reference'
                    LOGGER.exception(msg)
                    # raise Exception(msg)

                try:
                    gam_model, predict_gam, gam_info = sdm.get_gam(ncs_reference, PAmask, modelname=key)
                    stat_infos.append(gam_info)
                    response.update_status('GAM sucessfully trained', staus_nr + 5)
                except:
                    msg = 'failed to train GAM for %s' % (key)
                    LOGGER.exception(msg)

                try:
                    prediction = sdm.get_prediction(gam_model, ncs)
                    response.update_status('prediction done', staus_nr + 7)
                except:
                    msg = 'failed to predict tree occurence'
                    LOGGER.exception(msg)
                    # raise Exception(msg)

                # try:
                #     response.update_status('land sea mask for predicted data', staus_nr + 8)
                #     from numpy import invert, isnan, nan, broadcast_arrays  # , array, zeros, linspace, meshgrid
                #     mask = invert(isnan(PAmask))
                #     mask = broadcast_arrays(prediction, mask)[1]
                #     prediction[mask is False] = nan
                # except:
                #     LOGGER.exception('failed to mask predicted data')

                try:
                    species_files.append(sdm.write_to_file(ncs[0], prediction))
                    LOGGER.info('Favourabillity written to file')
                except:
                    msg = 'failed to write species file'
                    LOGGER.exception(msg)
                    # raise Exception(msg)
            except:
                msg = 'failed to process SDM chain for %s ' % key
                LOGGER.exception(msg)
                # raise Exception(msg)

        try:
            archive_references = archive(ncs_references, format=archive_format)
            LOGGER.info('indices 2D added to archive')
        except:
            msg = 'failed adding 2D indices to archive'
            LOGGER.exception(msg)
            archive_references = tempfile.mkstemp(suffix='.tar', prefix='foobar-', dir='.')

        try:
            archive_prediction = archive(species_files, format=archive_format)
            LOGGER.info('species_files added to archive')
        except:
            msg = 'failed adding species_files indices to archive'
            LOGGER.exception(msg)
            archive_predicion = tempfile.mkstemp(suffix='.tar', prefix='foobar-', dir='.')

        try:
            from flyingpigeon.visualisation import pdfmerge, concat_images
            stat_infosconcat = pdfmerge(stat_infos)
            LOGGER.debug('pngs %s' % PAmask_pngs)
            PAmask_png = concat_images(PAmask_pngs, orientation='h')
            LOGGER.info('stat infos pdfs and mask pngs merged')
        except:
            LOGGER.exception('failed to concat images')
            _, stat_infosconcat = tempfile.mkstemp(suffix='.pdf', prefix='foobar-', dir='.')
            _, PAmask_png = tempfile.mkstemp(suffix='.png', prefix='foobar-', dir='.')

        response.outputs['output_gbif'].file = occurence_map
        response.outputs['output_PA'].file = PAmask_png
        response.outputs['output_reference'].file = archive_references
        response.outputs['output_prediction'].file = archive_prediction
        response.outputs['output_info'].file = stat_infosconcat

        response.update_status('done', 100)
        return response
