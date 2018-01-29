import pytest

from pywps import Service
from pywps.tests import assert_response_success

from .common import client_for
from flyingpigeon.processes import processes


def test_caps():
    client = client_for(Service(processes=processes))
    resp = client.get(service='wps', request='getcapabilities', version='1.0.0')
    names = resp.xpath_text('/wps:Capabilities'
                            '/wps:ProcessOfferings'
                            '/wps:Process'
                            '/ows:Identifier')
    sorted_names = sorted(names.split())
    print sorted_names

    expected_names = [
        'EO_COPERNICUS_fetch',
        'EO_COPERNICUS_rgb',
        'EO_COPERNICUS_search',
        'analogs_compare',
        'analogs_model',
        'analogs_reanalyse',
        'analogs_viewer',
        # 'climatefactsheet',
        'fetch_resources',
        # 'indices_percentile,
        'indices_percentiledays',
        'indices_single',
        'landseamask',
        'map_spatial_analog',
        'plot_timeseries',
        'pointinspection',
        # 'robustness',
        'sdm_allinone',
        'sdm_csv',
        'sdm_csvindices',
        'sdm_gbiffetch',
        'sdm_getindices',
        'segetalflora',
        'spatial_analog',
        'subset_continents',
        'subset_countries',
        'subset_regionseurope',
        'weatherregimes_model',
        'weatherregimes_projection',
        'weatherregimes_reanalyse',
    ]
    assert sorted_names == expected_names
