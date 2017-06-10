from pywps import Process
from pywps import LiteralInput
from pywps import ComplexInput, ComplexOutput
from pywps import Format
from pywps.app.Common import Metadata

from flyingpigeon.indices import indices, indices_description
from flyingpigeon.indices import calc_indice_percentile
from flyingpigeon.subset import countries, countries_longname
from flyingpigeon.utils import GROUPING
from flyingpigeon.utils import rename_complexinputs
from flyingpigeon.utils import archive, archiveextract
from flyingpigeon import config
from flyingpigeon.log import init_process_logger

import logging
LOGGER = logging.getLogger("PYWPS")


class IndicespercentileProcess(Process):
    def __init__(self):
        inputs = [
            ComplexInput('resource', 'Resource',
                         abstract="NetCDF Files or archive (tar/zip) containing netCDF files",
                         min_occurs=1,
                         max_occurs=1000,
                         #  maxmegabites=5000,
                         supported_formats=[
                             Format('application/x-netcdf'),
                             Format('application/x-tar'),
                             Format('application/zip'),
                         ]),

            LiteralInput("indices", "Index",
                         abstract='Select an index',
                         default='TG',
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,  # len(indices()),
                         allowed_values=['TG', 'TN', 'TX'],  # indices()
                         ),

            LiteralInput("percentile", "Percentile",
                         abstract='Select an percentile',
                         default='90',
                         data_type='integer',
                         min_occurs=1,
                         max_occurs=1,  # len(indices()),
                         allowed_values=range(1, 100),  # indices()
                         ),

            LiteralInput("refperiod", "Reference Period",
                         abstract="Time refperiod to retrieve the percentile level",
                         default="19700101-20101231",
                         data_type='string',
                         min_occurs=0,
                         max_occurs=1,
                         ),
            #
            # self.refperiod = self.addLiteralInput(
            #     identifier="refperiod",
            #     title="Reference refperiod",
            #     abstract="Reference refperiod for climate condition (all = entire timeserie)",
            #     default=None,
            #     type=type(''),
            #     minOccurs=0,
            #     maxOccurs=1,
            #     allowedValues=['all','1951-1980', '1961-1990', '1971-2000','1981-2010']
            #     )

            LiteralInput("grouping", "Grouping",
                         abstract="Select an time grouping (time aggregation)",
                         default='yr',
                         data_type='string',
                         min_occurs=1,
                         max_occurs=1,
                         allowed_values=GROUPING
                         ),

            LiteralInput('region', 'Region',
                         data_type='string',
                         # abstract= countries_longname(), # need to handle special non-ascii char in countries.
                         abstract="Country ISO-3166-3:\
                          https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3#Officially_assigned_code_elements",
                         min_occurs=0,
                         max_occurs=len(countries()),
                         allowed_values=countries()),  # REGION_EUROPE #COUNTRIES

            LiteralInput("mosaic", "Mosaic",
                         abstract="If Mosaic is checked, selected polygons be clipped as a mosaic for each input file",
                         default='0',
                         data_type='boolean',
                         min_occurs=0,
                         max_occurs=1,
                         ),

        ]

        outputs = [
            ComplexOutput("output_archive", "Masked Files Archive",
                          abstract="Tar file of the masked netCDF files",
                          supported_formats=[Format("application/x-tar")],
                          as_reference=True,
                          ),

            # ComplexOutput("output_example", "Example",
            #               abstract="one example file to display in the WMS",
            #               supported_formats=[Format("application/x-netcdf")],
            #               as_reference=True,
            #               ),

            ComplexOutput('output_log', 'Logging information',
                          abstract="Collected logs during process run.",
                          as_reference=True,
                          supported_formats=[Format("text/plain")])
        ]

        super(IndicespercentileProcess, self).__init__(
            self._handler,
            identifier="indices_percentile",
            title="Climate indices (Percentile based)",
            version="0.10",
            abstract="Climate indices based on one single input variable\
             and the percentile of a reference period.",
            metadata=[
                {'title': 'Doc',
                 'href': 'http://flyingpigeon.readthedocs.io/en/latest/descriptions/\
                 index.html#climate-indices'},
                {"title": "ICCLIM",
                 "href": "http://icclim.readthedocs.io/en/latest/"},
                {"title": "Percentile-based indices", "href": "http://flyingpigeon.readthedocs.io/en/\
                latest/descriptions/indices.html#percentile-based-indices"},
            ],
            inputs=inputs,
            outputs=outputs,
            status_supported=True,
            store_supported=True
        )

    def _handler(self, request, response):
        init_process_logger('log.txt')
        response.outputs['output_log'].file = 'log.txt'

        try:
            resources = archiveextract(
                resource=rename_complexinputs(request.inputs['resource']))
            indices = request.inputs['indices'][0].data

            grouping = request.inputs['grouping'][0].data
            # grouping = [inpt.data for inpt in request.inputs['grouping']]

            if 'region' in request.inputs:
                region = request.inputs['region'][0].data
            else:
                region = None

            if 'mosaic' in request.inputs:
                mosaic = request.inputs['mosaic'][0].data
            else:
                mosaic = False

            percentile = request.inputs['percentile'][0].data
            refperiod = request.inputs['refperiod'][0].data

            from datetime import datetime as dt

            if refperiod is not None:
                start = dt.strptime(refperiod.split('-')[0], '%Y%m%d')
                end = dt.strptime(refperiod.split('-')[1], '%Y%m%d')
                refperiod = [start, end]

            # response.update_status('starting: indices=%s, grouping=%s, num_files=%s'
            #                        % (indices,  grouping, len(resources)), 2)

            LOGGER.debug("grouping %s " % grouping)
            LOGGER.debug("mosaic %s " % mosaic)
            LOGGER.debug("refperiod set to %s, %s " % (start, end))
            LOGGER.debug('indices= %s ' % indices)
            LOGGER.debug('percentile: %s' % percentile)
            LOGGER.debug('region %s' % region)
            LOGGER.debug('Nr of input files %s ' % len(resources))

        except:
            LOGGER.exception('failed to read in the arguments')

        from flyingpigeon.utils import sort_by_filename

        datasets = sort_by_filename(resources, historical_concatination=True)
        results = []
        try:
            for key in datasets.keys():
                try:
                    result = calc_indice_percentile(resource=datasets[key],
                                         indices=indices,
                                         percentile=percentile,
                                         mosaic=mosaic,
                                         polygons=region,
                                         refperiod=refperiod,
                                         grouping=grouping,
                                         # dir_output=os.curdir,
                                         )
                    LOGGER.debug('percentile based indice done for %s' % result)
                    results.extend(result)
                except:
                    LOGGER.exception("failed to calculate percentil based indice for %s " % key)
        except:
            LOGGER.exception("failed to calculate percentile indices")

        output_archive = archive(results)

        response.outputs['output_archive'].file = output_archive
        response.update_status("done", 100)
        return response
