from utils import get_variable, get_values, get_time
import config
import os
from os.path import join, basename
from shutil import copyfile

import logging
LOGGER = logging.getLogger("PYWPS")

from cdo import Cdo
cdo_version = Cdo().version()
cdo = Cdo(env=os.environ)

import pandas


def select_run_tokeep(f_xhist, f_xrcp, f_yhist, f_yrcp):
    run_xrcp = [basename(f).split("_")[4] for f in f_xrcp]
    run_xhist = [basename(f).split("_")[4] for f in f_xhist]
    run_yrcp = [basename(f).split("_")[4] for f in f_yrcp]
    run_yhist = [basename(f).split("_")[4] for f in f_yhist]
    rset_xrcp = set(run_xrcp)
    rset_xhist = set(run_xhist)
    rset_yrcp = set(run_yrcp)
    rset_yhist = set(run_yhist)
    rset_all = set(run_xhist + run_xrcp + run_yhist + run_yrcp)
    rset_others = [rset_xhist, rset_yrcp, rset_yhist]
    run_tokeep = rset_xrcp.intersection(*rset_others)
    return run_tokeep

def select_file_tokeep(lf, run_tokeep):
    run = [basename(f).split("_")[4] for f in lf]
    lf_tokeep = [x for (x, y) in zip(lf, run) if y in run_tokeep]

def strings_to_spagg(argument):
    switcher = {
        "mean": cdo.fldmean,
        "max": cdo.fldmax,
        "min": cdo.fldmin,
    }
    return switcher.get(argument, "nothing")

def strings_to_tagg(argument):
    switcher = {
        "mean": cdo.yearmean,
        "max": cdo.yearmax,
        "min": cdo.yearmin,
    }
    return switcher.get(argument, "nothing")

def prepare_dat(varname, 
                lfhist, lfrcp, run_tokeep,
                bbox,
                season,
                compute_ano = True,
                start_ano = "1850-01-01T00:00:00",
                end_ano = "2100-12-31T23:59:59",
                first_spatial = True,
                spatial_aggregator = "mean",
                time_aggregator = "mean"
                ):
    import os
    run_rcp = [basename(f).split("_")[4] for f in lfrcp]
    run_hist = [basename(f).split("_")[4] for f in lfhist]
    fhist_tokeep = [x for (x, y) in zip(lfhist, run_hist) if y in run_tokeep]
    frcp_tokeep = [x for (x, y) in zip(lfrcp, run_rcp) if y in run_tokeep]
    f_agg = {}
    drun = {}
    file_torm = []
    print run_tokeep
    for r in run_tokeep:
        print r
        LOGGER.debug('o_O!!!') 
        f_onerun_rcp = [x for (x, y) in zip(lfrcp, run_rcp) if y == r]
        f_onerun_hist = [x for (x, y) in zip(lfhist, run_hist) if y == r]
        drun[r] =  sorted(f_onerun_hist + f_onerun_rcp)
        LOGGER.debug('cdo merge time') 
        cdo.mergetime(input = " ".join(drun[r]), output = "tmp1.nc", options = "-b F64")
        LOGGER.debug('cdo sellonlatbox') 
        cdo.sellonlatbox(bbox, input = "tmp1.nc", output = "tmp2.nc", options = "-b F64") 
        LOGGER.debug('cdo selseason') 
        cdo.select('season=' + season, input = "tmp2.nc", output = "tmp1.nc", options = "-b F64")
        if compute_ano:
            LOGGER.debug('cdo compute_ano') 
            # maybe offer anomalies computation only on a given period with cdo seldate,startdate,endate
            cdo.ydaysub(input = "tmp1.nc" +
                    " -ydaymean -seldate," + start_ano + "," + end_ano + " "  + "tmp1.nc",
                    output = "tmp2.nc", options = "-b F64")
            copyfile("tmp2.nc", "tmp1.nc")
        if first_spatial:
            LOGGER.debug('cdo first_spatial') 
            strings_to_spagg(spatial_aggregator)(input = "tmp1.nc",
                    output = "tmp2.nc",
                    options = "-b F64")
            strings_to_tagg(time_aggregator)(input = "tmp2.nc",
                    output = "tmp1.nc",
                    options = "-b F64")
        else: 
            LOGGER.debug('cdo first_temporal') 
            strings_to_tagg(time_aggregator)(input = "tmp1.nc",
                    output = "tmp2.nc",
                    options = "-b F64")
            strings_to_spagg(spatial_aggregator)(input = "tmp2.nc",
                    output = "tmp1.nc",
                    options = "-b F64")
        f_agg[r] = "agg_" + r + ".nc"
        copyfile("tmp1.nc", f_agg[r])         
        LOGGER.debug('cdo run end!!!') 
        # print cdo.sinfo(input = temp_nc)
    lf_agg = [f_agg[r] for r in run_tokeep]
    time = get_time(lf_agg)
    year = [t.year for t in time]
            # add xname and yname as arguments
    if len(run_tokeep) == 1:
        var = get_values(lf_agg[0], variable = varname)
    else:
            # add xname and yname as arguments
        var = get_values(lf_agg, variable = varname)
    #print file_torm
    #for f in file_torm:
    #    os.remove(f)
    import pandas
    ps_year = pandas.Series(year)
    counts_year = ps_year.value_counts()
    var, year = map(list,zip(*[(x,y) for x,y  in zip(var, year) if counts_year[y] == len(run_tokeep)]))
    return var, year

def compute_far(plot_pdf, data_rds, 
                yvarname, xvarname,
                f_yhist, f_yrcp,
                f_xhist, f_xrcp,
                y_compute_ano = True,
                y_start_ano = "1961-01-01T00:00:00",
                y_end_ano = "1990-12-31T23:59:59",
                y_bbox = '-127,-65,25,50',
                y_season = 'DJF',
                y_first_spatial = True,
                y_spatial_aggregator = "mean",
                y_time_aggregator = "mean",
                x_compute_ano = True,
                x_start_ano = "1961-01-01T00:00:00",
                x_end_ano = "1990-12-31T23:59:59",
                x_bbox = '-127,-65,25,50',
                x_season = 'DJF',
                x_first_spatial = True,
                x_spatial_aggregator = "mean",
                x_time_aggregator = "mean",
                stat_model = "gauss_fit",
                qthreshold = 0.9,
                nbootstrap = 250
               ):
    
    LOGGER.debug('initialization') 
    xname = "x_" + xvarname
    yname = "y_" + yvarname
    LOGGER.debug('bug0!!!') 

    run_tokeep = select_run_tokeep(f_xhist, f_xrcp, f_yhist, f_yrcp)
    xvar, xyear = prepare_dat(varname = xvarname,
                              lfhist = f_xhist,
                              lfrcp = f_xrcp,
                              run_tokeep = run_tokeep,
                              compute_ano = x_compute_ano,
                              start_ano = x_start_ano,
                              end_ano = x_end_ano,
                              first_spatial = y_first_spatial,
                              spatial_aggregator = x_spatial_aggregator,
                              time_aggregator = x_time_aggregator,
                              bbox = x_bbox,
                              season = x_season)

    LOGGER.debug('bug2!!!') 
    yvar, yyear = prepare_dat(varname = yvarname,
                              lfhist = f_yhist,
                              lfrcp = f_yrcp,
                              run_tokeep = run_tokeep,
                              compute_ano = y_compute_ano,
                              start_ano = y_start_ano,
                              end_ano = y_end_ano,
                              first_spatial = y_first_spatial,
                              spatial_aggregator = y_spatial_aggregator,
                              time_aggregator = y_time_aggregator,
                              bbox = y_bbox,
                              season = y_season)


    LOGGER.info('data prepared') 
    dfx = {}
    dfx['year'] =  xyear
    dfx[xname] =  xvar
    dfx = pandas.DataFrame.from_dict(dfx)
    dfy = {}
    dfy['year'] = yyear
    dfy[yname] = yvar
    dfy = pandas.DataFrame.from_dict(dfy)
    if all(dfx['year'] == dfy['year']):
        df = pandas.concat([dfx, dfy[yname]], axis = 1)[['year', yname, xname]]
    else:  
        raise Exception("years of x and y not corresponding")

    Rsrc = config.Rsrc_dir()
    # import rpy2's package module
    import rpy2.robjects.packages as rpackages
    from rpy2.robjects import pandas2ri
    pandas2ri.activate()
    from rpy2.robjects import r
    from rpy2.robjects.packages import importr
    # import R's utility package
    utils = importr('utils')
    r.source(join(Rsrc, "compute_and_plot_far.R"))
    farg = importr("FARg")
    LOGGER.debug('rcode prepared') 

    if(stat_model == "gauss_fit"):
        far = r.compute_and_plot_far(mdata = df, yvar=yname, xvar=xname, tvar="year", xp=1.6 , R=nbootstrap, stat_model=farg.gauss_fit, ci_p=0.9, pdf_name = plot_pdf)
    if(stat_model == "gev_fit"):
        far = r.compute_and_plot_far(mdata = df, yvar=yname, xvar=xname, tvar="year", xp=1.6 , R=nbootstrap, stat_model=farg.gev_fit, ci_p=0.9, pdf_name = plot_pdf)
    if(stat_model == "gpd_fit"):
        far = r.compute_and_plot_far(mdata = df, yvar=yname, xvar=xname, tvar="year", xp=1.6 , R=nbootstrap, stat_model=farg.gpd_fit, ci_p=0.9, qthreshold = qthreshold, pdf_name = plot_pdf)
    
    LOGGER.debug('far computed') 

    r.saveRDS(far, file = data_rds)
