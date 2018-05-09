"""
Processes with cdo commands
"""

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexInput, ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata

from os import environ, path, chmod

from cdo import Cdo
cdo_version = Cdo().version()

from blackswan import config

import logging
LOGGER = logging.getLogger("PYWPS")


class Inter_Sub(Process):

    def __init__(self):
        inputs = [
            ComplexInput('netcdf_file', 'NetCDF File',
                         abstract='You may provide a URL or upload a NetCDF file.',
                         metadata=[Metadata('Info')],
                         min_occurs=1,
                         max_occurs=100,
                         supported_formats=[Format('application/x-netcdf')]),
            LiteralInput('operator', 'CDO Operator',
                         data_type='string',
                         abstract="Choose a CDO Operator",
                         default='remapbil',
                         min_occurs=0,
                         max_occurs=1,
                         allowed_values=['remapbil', 'remapbic', 'remapdis',
                                         'remapnn', 'remapcon', 'remapcon2', 'remaplaf']),
        ]

        outputs = [
            ComplexOutput('tarout', 'Result files',
                          abstract="Tar archive containing the netCDF result files",
                          as_reference=True,
                          supported_formats=[Format('application/x-tar')]),
            ComplexOutput('output', 'Output',
                          abstract="One regrided file.",
                          as_reference=True,
                          supported_formats=[Format('application/x-netcdf')]),
        ]

        super(Inter_Sub, self).__init__(
            self._handler,
            identifier="regrsub",
            title="CDO Remapping and Subsetting",
            abstract="CDO Remapping and Subsetting of NetCDF File(s)",
            version=cdo_version,
            metadata=[
                Metadata('Birdhouse', 'http://bird-house.github.io/'),
                Metadata('User Guide', 'http://birdhouse-hummingbird.readthedocs.io/en/latest/'),
                Metadata('CDO Homepage', 'https://code.zmaw.de/projects/cdo'),
                Metadata('CDO Documentation', 'https://code.zmaw.de/projects/cdo/embedded/index.html'),
            ],
            inputs=inputs,
            outputs=outputs,
            status_supported=True,
            store_supported=True,
        )

    def _handler(self, request, response):
        import tarfile
        import tempfile

        nc_files = []
        for dataset in request.inputs['netcdf_file']:
            nc_files.append(dataset.file)

        if type(nc_files) == list:
            nc_files = sorted(nc_files, key=lambda i: path.splitext(path.basename(i))[0])
        else:
            nc_files = [nc_files]

        (fp_tarf, tarf) = tempfile.mkstemp(dir=".", suffix='.tar')
        tar = tarfile.open(tarf, "w")

        cdo = Cdo(env=environ)
        # operator='remapbil'
        operator = request.inputs['operator'][0].data

        gri = path.join(config.masks_path(), 'EUR_1x1.nc')

        cdo_op = getattr(cdo, operator)

        for nc_file in nc_files:
            LOGGER.debug('Input NetCDF file = %s' % (nc_file))

            # (fp_ncf, outfile) = tempfile.mkstemp(dir=".", suffix='.nc')

            new_arc_name = path.basename(nc_file.split(".nc")[0] + "_" + operator + "_EU1x1" + ".nc")
            outfile = new_arc_name

            cdo_op(gri, input=nc_file, output=outfile)

            LOGGER.debug('NEW NAME for Output NetCDF file = %s' % (new_arc_name))
            tar.add(outfile, arcname=new_arc_name)

        tar.close()
        chmod(tarf, 0644)
        response.outputs['output'].file = outfile
        response.outputs['tarout'].file = tarf
        response.update_status("cdo remapping and subsetting done", 100)
        return response
