import pytest

from .common import TESTDATA

import os
import tempfile
from netCDF4 import Dataset

from flyingpigeon import indices
from flyingpigeon.utils import local_path


def test_indices():
    assert 'TG' in indices.indices()


def test_indices_description():
    assert 'TG: ' in indices.indices_description()


@pytest.mark.skip(reason="no way of currently testing this")
def test_indice_simple():
    # SU expects tasmax
    resources = [local_path(TESTDATA['cordex_tasmax_2006_nc'])]
    output = indices.calc_indice_simple(
        resources,
        indice='SU', grouping='yr', dir_output=tempfile.mkdtemp())

    assert os.path.basename(output[0]) == 'SU_EUR-44_MPI-M-MPI-ESM-LR_rcp45_r1i1p1_MPI-CSC-REMO2009_v1_mon_200602-200612.nc'  # noqa

    with Dataset(output[0]) as ds:
        # SU variable must be in result
        assert 'SU' in ds.variables
        # 1 year
        assert len(ds.variables['time']) == 1

@pytest.mark.skip
def test_indice_percentile():
    # TX90p expects tasmax
    resources = [local_path(TESTDATA['cordex_tasmax_2006_nc'])]
    output = indices.calc_indice_percentile(
        resources, variable='tasmax',
        indices=['TX'], percentile=90, grouping='yr',
        dir_output=tempfile.mkdtemp())

    assert os.path.basename(output[
                                0]) == 'SU_EUR-44_MPI-M-MPI-ESM-LR_rcp45_r1i1p1_MPI-CSC-REMO2009_v1_mon_200602-200612.nc'  # noqa
    with Dataset(output) as ds:
        # SU variable must be in result
        assert 'TX' in ds.variables
        # 1 year
        assert len(ds.variables['time']) == 1
