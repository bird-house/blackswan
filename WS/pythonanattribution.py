import logging
LOGGER = logging.getLogger("PYWPS")

import pandas
import random
import numpy as np

def analogs_generator(anafile, yfile, nsim = 20):
    # Simulates nsim values of the variable y using analogues for all the dates present in the file anafile
    # anafile path to a file with the results of the analogues
    # yfile path to the file containing the data. The file should have two columns: 
    # - the first with the date with the following for format yyyymmdd
    # - the second with the variable of interest y, columns are separated by spaces and are supposed to have headers
    # nsim the number of simulations of the variablle y to generate with the analogues
    def weight_analogues(date):
        dist = disttable.loc[[date], :].transpose()
        date = anatable.loc[[date], :].transpose()
        weights = pandas.concat([date.reset_index(drop=True), dist.reset_index(drop=True)], axis = 1)
        weights.columns = ['date', 'dist']
        weights = weights.set_index('date')
        return weights
    def select_y_analogues(date):
        bidx = ytable.index.isin(anatable.loc[date, :])
        return ytable.iloc[bidx, 0] 
    def generate_cond_ymean(date, nsim = 20):
        weights = weight_analogues(date)
        ys = select_y_analogues(date)
        dat = pandas.concat([ys, weights], axis = 1, join = "inner")
        weights = np.random.multinomial(nsim, dat.dist / sum(dat.dist))
        return random.sample(np.repeat(dat.iloc[:, 0], weights), nsim)
    ytable = pandas.read_table(yfile, sep = " ", skipinitialspace = True)
    anatable = pandas.read_table(anafile, sep = " ", skipinitialspace = True)
    nanalogs = len([s for s in anatable.columns if "dis" in s])
    disttable = anatable.iloc[:, [0] + range(nanalogs + 1, 2 * nanalogs + 1)].copy()
    cortable = anatable.iloc[:, [0] + range(2 * nanalogs + 1, 3 * nanalogs + 1)].copy()
    anatable = anatable.iloc[:, 0:(nanalogs + 1)].copy()
    ytable = ytable.set_index('date')
    disttable = disttable.set_index('date')
    cortable = cortable.set_index('date')
    anatable = anatable.set_index('date')
    condys = map(generate_cond_ymean, anatable.index, np.repeat(nsim, len(anatable.index)))
    condys = pandas.DataFrame(condys)
    condys = condys.transpose()
    #condys = [x.reset_index(drop=True) for x in condys]
    #condys = pandas.concat(condys, axis = 1)
    condys.columns = anatable.index
    return condys
    #condyms = condys.mean(axis=1)
    #return condyms
