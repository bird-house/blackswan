from os.path import join, abspath, dirname, getsize, curdir, isfile
from netCDF4 import Dataset
from blackswan import config
import logging
LOGGER = logging.getLogger("PYWPS")

# == eggshell ==
# def has_Lambert_Conformal(resource):
#     """
#     Check if grid is organised as Lambert_Conformal
# 
#     :param resource: file to be checked
# 
#     :return Boolean: True/False
#     """
#     if type(resource) != list:
#         resource = [resource]
#     for nc in resource:
#         ds = Dataset(nc)
#         if 'Lambert_Conformal' not in ds.variables.keys():
#             return False
#     return True


def call(resource=[], variable=None, dimension_map=None, agg_selection=True, calc=None,
         calc_grouping=None, conform_units_to=None, crs=None, memory_limit=None,  prefix=None,
         regrid_destination=None, regrid_options='bil', level_range=None, cdover='python',
         geom=None, output_format_options=None, search_radius_mult=2.,
         select_nearest=False, select_ugid=None, spatial_wrapping=None,
         t_calendar=None, time_region=None,
         time_range=None, dir_output=None, output_format='nc'):
    '''
    ocgis operation call

    :param resource:
    :param variable: variable in the input file to be picked
    :param dimension_map: dimension map in case of unconventional storage of data
    :param agg_selection: For aggregation of in case of mulitple polygons geoms
    :param calc: ocgis calc syntax for calculation partion
    :param calc_grouping: time aggregate grouping
    :param cdover: use py-cdo ('python', by default) or cdo from the system ('system')
    :param conform_units_to:
    :param crs: coordinate reference system
    :param memory_limit: limit the amount of data to be loaded into the memory at once \
        if None (default) free memory is detected by birdhouse
    :param level_range: subset of given levels
    :param prefix: string for the file base name
    :param regrid_destination: file path with netCDF file with grid for output file
    :param geom: name of shapefile stored in birdhouse shape cabinet
    :param output_format_options: output options for netCDF e.g compression level()
    :param regrid_destination: file containing the targed grid (griddes.txt or netCDF file)
    :param regrid_options: methods for regridding:
                          'bil' = Bilinear interpolation
                          'bic' = Bicubic interpolation
                          'dis' = Distance-weighted average remapping
                          'nn' = nearest neighbour
                          'con' = First-order conservative remapping
                          'laf' = largest area fraction reamapping
    :param search_radius_mult: search radius for point geometries. All included gridboxes will be returned
    :param select_nearest: nearest neighbour selection for point geometries
    :param select_ugid: ugid for appropriate polygons
    :param spatial_wrapping: how to handle coordinates in case of subsets, options: None (default), 'wrap', 'unwrap'
    :param time_region: select single month
    :param time_range: sequence of two datetime.datetime objects to mark start and end point
    :param dir_output (default= curdir):
    :param output_format:
    :return: output file path
    '''
    LOGGER.info('Start ocgis module call function')
    from ocgis import OcgOperations, RequestDataset, env, DimensionMap, crs
    from ocgis.util.large_array import compute
    from datetime import datetime as dt
    from datetime import date as dd
    from datetime import time as dt_time
    import uuid

    # prepare the environment
    env.DIR_SHPCABINET = config.shapefiles_path()
    env.OVERWRITE = True
    # env.DIR_OUTPUT = dir_output
    # LOGGER.debug(' **** env.DIR_OUTPUT  = %s ' % env.DIR_OUTPUT)

    if dir_output is None:
        dir_output = abspath(curdir)

    # check time_range format:

    if time_range is not None:
        try:
            LOGGER.debug('time_range type= %s , %s ' % (type(time_range[0]), type(time_range[1])))
            LOGGER.debug('time_range= %s , %s ' % (time_range[0], time_range[1]))
            # if type(time_range[0] is 'datetime.date'):
            if (isinstance(time_range[0], dd) and not isinstance(time_range[0], dt)):
                time_range = [dt.combine(time_range[0], dt.min.time()),
                              dt.combine(time_range[1], dt.min.time())]
                # time_range = [dt.combine(time_range[0], dt_time(12,0)),
                #               dt.combine(time_range[1], dt_time(12,0))]
            LOGGER.debug('time_range changed to type= %s , %s ' % (type(time_range[0]), type(time_range[1])))
            LOGGER.debug('time_range changed to= %s , %s ' % (time_range[0], time_range[1]))
        except:
            LOGGER.exception('failed to confert data to datetime')

    if spatial_wrapping == 'wrap':
        spatial_reorder = True
    else:
        spatial_reorder = False
    LOGGER.debug('spatial_reorder: %s and spatial_wrapping: %s ' % (spatial_reorder, spatial_wrapping))

    if prefix is None:
        prefix = str(uuid.uuid1())
        env.PREFIX = prefix
    #
    # if output_format_options is False:
    #     output_format_options = None
    # elif output_format_options is True:
    #     output_format_options = {'data_model': 'NETCDF4',  # NETCDF4_CLASSIC
    #                              'variable_kwargs': {'zlib': True, 'complevel': 9}}
    # else:
    if output_format_options is not None:
        LOGGER.info('output_format_options are set to %s ' % (output_format_options))

    if type(resource) != list:
        resource = list([resource])
    # execute ocgis
    LOGGER.info('Execute ocgis module call function')

    # # needed for some AFR-44 data
    # dimension_map = DimensionMap()
    # dimension_map.set_variable('x', 'lon', dimension='rlon')
    # dimension_map.set_variable('y', 'lat', dimension='rlat')
    # dimension_map.set_variable('time', 'time', dimension='time')
    # crs=crs.Spherical()

    try:
        LOGGER.debug('call module curdir = %s ' % abspath(curdir))
        rd = RequestDataset(resource, variable=variable, level_range=level_range,
                            dimension_map=dimension_map, conform_units_to=conform_units_to,
                            time_region=time_region, t_calendar=t_calendar, time_range=time_range) # , crs=crs)

        from ocgis.constants import DimensionMapKey
        rd.dimension_map.set_bounds(DimensionMapKey.TIME, None)

        ops = OcgOperations(dataset=rd,
                            output_format_options=output_format_options,
                            dir_output=dir_output,
                            spatial_wrapping=spatial_wrapping,
                            spatial_reorder=spatial_reorder,
                            # regrid_destination=rd_regrid,
                            # options=options,
                            calc=calc,
                            calc_grouping=calc_grouping,
                            geom=geom,
                            agg_selection=agg_selection,
                            output_format=output_format,
                            prefix=prefix,
                            search_radius_mult=search_radius_mult,
                            select_nearest=select_nearest,
                            select_ugid=select_ugid,
                            add_auxiliary_files=False)
        LOGGER.info('OcgOperations set')
    except:
        LOGGER.exception('failed to setup OcgOperations')
        return None

    try:
        LOGGER.info('ocgis module call as ops.execute()')
        geom_file = ops.execute()
    except:
        LOGGER.exception('failed to execute ocgis operation')
        return None

    #
    # try:
    #     from numpy import sqrt
    #     from blackswan.utils import FreeMemory
    #
    #     if memory_limit is None:
    #         f = FreeMemory()
    #         mem_kb = f.user_free
    #         mem_mb = mem_kb / 1024.
    #         mem_limit = mem_mb / 2.  # set limit to half of the free memory
    #     else:
    #         mem_limit = memory_limit
    #
    #     if mem_limit >= 1024. * 4:
    #         mem_limit = 1024. * 4
    #         # 475.0 MB for openDAP
    #
    #     LOGGER.info('memory_limit = %s Mb' % (mem_limit))
    #
    #     data_kb = ops.get_base_request_size()['total']
    #     data_mb = data_kb / 1024.
    #
    #     # data_kb = size['total']/reduce(lambda x,y: x*y,size['variables'][variable]['value']['shape'])
    #     LOGGER.info('data_mb  = %s Mb' % (data_mb))
    #
    #     if data_mb <= mem_limit:  # input is smaler than the half of free memory size
    #         try:
    #             LOGGER.info('ocgis module call as ops.execute()')
    #             geom_file = ops.execute()
    #         except Exception as e:
    #             LOGGER.debug('failed to execute ocgis operation')
    #             raise
    #             return None
    #
    #     else:
    #         ##########################
    #         # calcultion of chunk size
    #         ##########################
    #         try:
    #             size = ops.get_base_request_size()
    #             nb_time_coordinates_rd = size['variables'][variable]['temporal']['shape'][0]
    #             element_in_kb = size['total']/reduce(lambda x, y: x*y, size['variables'][variable]['value']['shape'])
    #             element_in_mb = element_in_kb / 1024.
    #             tile_dim = sqrt(mem_limit/(element_in_mb*nb_time_coordinates_rd))  # maximum chunk size
    #
    #             LOGGER.info('ocgis module call compute with chunks')
    #             if calc is None:
    #                 calc = '%s=%s*1' % (variable, variable)
    #                 LOGGER.info('calc set to = %s ' % calc)
    #             ops = OcgOperations(dataset=rd,
    #                                 output_format_options=output_format_options,
    #                                 dir_output=dir_output,
    #                                 spatial_wrapping=spatial_wrapping,
    #                                 spatial_reorder=spatial_reorder,
    #                                 # regrid_destination=rd_regrid,
    #                                 # options=options,
    #                                 calc=calc,
    #                                 calc_grouping=calc_grouping,
    #                                 geom=geom,
    #                                 output_format=output_format,
    #                                 prefix=prefix,
    #                                 search_radius_mult=search_radius_mult,
    #                                 select_nearest=select_nearest,
    #                                 select_ugid=select_ugid,
    #                                 add_auxiliary_files=False)
    #             geom_file = compute(ops, tile_dimension=int(tile_dim), verbose=True)
    #             print 'ocgis calculated'
    #         except Exception as e:
    #             LOGGER.debug('failed to compute ocgis with chunks')
    #             raise
    #             return None
    #     LOGGER.info('Succeeded with ocgis module call function')
    # except:
    #     LOGGER.exception('failed to compare dataload with free memory, calling as execute instead')

    ############################################
    # remapping according to regrid informations
    ############################################
    if regrid_destination is not None:
        try:
            if (cdover=='system'):
                from os import system
                LOGGER.debug('Trying user cdo')
                remap = 'remap%s' % regrid_options
                output = '%s.nc' % uuid.uuid1()
                output = abspath(curdir)+'/'+output
                comcdo = 'cdo -O %s,%s %s %s' % (remap, regrid_destination, geom_file, output)
                system(comcdo)
                
                if(isfile(output)==False):
                    LOGGER.debug('User cdo failed, trying system')
                    comcdo = '/usr/bin/cdo -O %s,%s %s %s' % (remap, regrid_destination, geom_file, output)
                    system(comcdo)
                
                if(isfile(output)==False):
                    cdover='python'
                    LOGGER.debug('System cdo failed, trying python cdo')

                # need to substitute by subprocess call
                # TODO: If system failed - py-cdo used insted
                # what if py-cdo failed, with option 'python'
                # need to call 'system' in this case - need to write function

            if (cdover=='python'):
                from tempfile import mkstemp
                from cdo import Cdo
                from os import environ
                cdo = Cdo(env=environ)
                LOGGER.debug('Trying python cdo')
                output = '%s.nc' % uuid.uuid1()
                remap = 'remap%s' % regrid_options
                call = [op for op in dir(cdo) if remap in op]
                cmd = "output = cdo.%s('%s',input='%s', output='%s')" \
                      % (str(call[0]), regrid_destination, geom_file, output)
                exec cmd
        except Exception as e:
            LOGGER.debug('failed to remap')
            raise
            return None
    else:
        output = geom_file

    # try:
    #     from blackswan.utils import unrotate_pole
    #     lat, lon = unrotate_pole(output)
    # except:
    #     LOGGER.exception('failed to unrotate pole')
    return output

# == eggshell ==
# def eval_timerange(resource, time_range):
#     """
#     quality checker if given time_range is covered by timesteps in resource files
# 
#     :param resource: input netCDF files
#     :param time_range: start and end date of time range [datetime,datetime]
# 
#     :returns [datetime,datetime]: time_range
#     """
#     from blackswan.utils import get_time
# 
#     LOGGER.info('time_range: %s' % time_range)
# 
#     if type(resource) != str:
#         resource.sort()
#     time = get_time(resource)
#     start = time[0]
#     end = time[-1]
# 
#     if (time_range[0] > start or time_range[0] < end):
#         LOGGER.debug('time range start %s not in input dataset covering: %s to %s' % (time_range[0], start, end))
#         time_range[0] = start
#     LOGGER.debug('time_range start changed to first timestep of dataset')
#     if (time_range[1] > end or time_range[1] < start):
#         LOGGER.debug('time range end %s not in input dataset covering: %s to %s' % (time_range[0], start, end))
#         time_range[1] = end
#     LOGGER.debug('time_range end changed to last timestep of dataset')
#     if (time_range[0] > time_range[1]):
#         time_range = reversed(time_range)
#         LOGGER.debug('time range reversed! start was later than end ')
#     LOGGER.info('time range start and end set')
#     return time_range

# == eggshell == obsolete ?
# # check memory load
# from os import stat
#   if memory_limit == None:
#     f = FreeMemory()
#     mem_kb = f.user_free
#     mem_mb = mem_kb / 1024.
#     mem_limit = mem_mb / 2. # set limit to half of the free memory
#   else:
#     mem_limit = memory_limit
#
#   if mem_limit >= 1024. * 4:
#     mem_limit = 1024. * 4
#     # 475.0 MB for openDAP
#
#   #if type(resource) == list :
#     #data_kb =  stat(resource[0]).st_size * len(resource)
#   #else:
#     #data_kb =  stat(resource).st_size
#   size = ops.get_base_request_size()['total']
#   data_kb = size['total']/reduce(lambda x,y: x*y,size['variables'][variable]['value']['shape'])
#   data_mb = data_kb / 1024.
#
#   if variable == None:
#     variable = rd.variable
#     LOGGER.info('%s as variable dedected' % (variable))
