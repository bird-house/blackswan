# Computation of the local dimension and persistence.
# Method Faranda et al, 2016 Scientific Reports
# attention!!!!!! dat must be always(time, space), 
# for instance: NCEP(1948-2014) ->> dat=(1:24418,1:1060), where time=24418 and gridpoints=1060 
# 
# based on Matlab code of Davide Faranda, davide.faranda@lsce.ipsl.fr
# and adapted by Carmen Alvarez-Castro, carmen.alvarez-castro@cmcc.it
# Rome, November 2017
#
# New version: new calculation of theta 
# LSCE, May 2018

rm(list=ls())
library(ncdf4)
library(pracma)

args=(commandArgs(TRUE))
print(args)

# /homel/nkadyg/birdhouse/davide/slp.1948-1950_NA.nc

# model="NCEP"

lonname="lon"
latname="lat"
timename="time"

if(length(args)>0){
    fname=args[1]
    varname=args[2]
    dim_fn=args[3]
}else{
    DATdir="/homel/nkadyg/birdhouse/davide/"
    varname="slp"
    dim_fn = "NCEP.txt"
    yr1=1948
    #yr2=1948
    yr2=1950
    #yr2=2014
    region="NA"
    fname = paste(DATdir,varname,".",yr1,"-",yr2,"_",region,".nc",sep="")
}

# open netCDF
nc = nc_open(fname)
data=ncvar_get(nc,varname)
lon=ncvar_get(nc,lonname)
lat=ncvar_get(nc,latname)
time=ncvar_get(nc,timename)
nx=dim(data)[2];ny=dim(data)[1]
nt=dim(data)[3]
# reshape order lat-lon-time
dat = data*NA; dim(dat) <- c(nt,ny,nx)
for (i in 1:nt) dat[i,,] <- t(as.matrix(data[,,i]))
# two dimentions
dim(dat)=c(nt,nx*ny)
nc_close(nc)
#
# closing netcdf
print(paste("Processing",fname))

# Quantile definition
quanti=0.98
npoints=nrow(dat)
dim=numeric(npoints)
theta=numeric(npoints)

# ptm <- proc.time()
# dist=pdist2(dat,dat)
# print(proc.time() - ptm)

# =======================================================
# Function to calculate dist and theta
# =======================================================
calcmethedist <- function(tmp_x) {
  # week point here.. 
  # dat,npoints and quanti are global variables...
  # which is not good and not modulate
  # But this function is wrapper, so I need 1 arg only

  # tmp_npoints=length(tmp_x)
  # tmp_y=dat

  tmp_npoints=npoints
  tmp_quanti=quanti

  tmp_ress=numeric(2)
  # tmp_distance=pdist2(tmp_x,tmp_y)
  tmp_distance=pdist2(tmp_x,dat)

  tmp_distance[tmp_distance<1] <- 0
  tmp_logdista=-log(tmp_distance)
  tmp_logdista = tmp_logdista*is.finite(tmp_logdista)
  # 3 ISSUEs HERE!
  # 1. 
  # matlab and python use quantile type = 5 by default
  # R use type = 7 by default
  # 2.
  # There are no 'NAN' in logdista but 'INF', so na.rm=TRUE wont work
  # One needs to modiefy logddista:   logdista = logdiata*is.finite(logdista)
  # And after calculate quantile
  # 3. 
  # For CMIP5 models distance between point with itself is not 0!, but some small positive value
    
  tmp_thresh=quantile(tmp_logdista, tmp_quanti, type = 5, na.rm=TRUE)
  tmp_Li<-which(tmp_logdista>tmp_thresh)
  tmp_Ti<-diff(tmp_Li)
  tmp_N=length(tmp_Ti)
  tmp_q=1-tmp_quanti
  tmp_Si=tmp_Ti-1
  tmp_Nc=length(which(tmp_Si>0))
  tmp_N=length(tmp_Ti)
  tmp_theta=(sum(tmp_q*tmp_Si)+tmp_N+tmp_Nc-sqrt(((sum(tmp_q*tmp_Si)+tmp_N+tmp_Nc)^2)-8*tmp_Nc*sum(tmp_q*tmp_Si)))/(2*sum(tmp_q*tmp_Si))
  tmp_ress[1]=tmp_theta
  tmp_logdista=sort(tmp_logdista)
  tmp_findidx=which(tmp_logdista>tmp_thresh)
  # tmp_logextr=tmp_logdista[tmp_findidx[[1]]:(length(tmp_logdista)-1)]
  tmp_logextr=tmp_logdista[tmp_findidx[[1]]:(length(tmp_logdista))]
  tmp_dim=1/mean(tmp_logextr-tmp_thresh)
  tmp_ress[2]=tmp_dim
  # print(tmp_ress)
  return(tmp_ress)
}
# =======================================================


# may be not needed, did't check yet - left for mcapply
library(parallel)

# need to be installed
# inside R run: install.packages("future.apply")
library("future.apply")
plan(multiprocess)

# experimental (!!!)
# uncomment vvv for bigger chunks! but may overload the system
# options(future.globals.maxSize=1073741824)

# may be not needed, did't check yet the performance
library(compiler)
MYcalcmethedist <- cmpfun(calcmethedist)

wrap=lapply(seq_len(nrow(dat)), function(i) dat[i,])

# Main Calc
# TODO NEED UPDATE: there is future_apply now in the latest pack
# to avoid converting to lists
bigres=future_lapply(wrap,MYcalcmethedist)

# Combine results
# here need apply function as well...
# instead of for loop which one needs to avoid
for(l in 1:npoints){
  # print for debug (to check with serial res)
  #print(bigres[[l]][1])
  theta[l]=bigres[[l]][1]
  #print(bigres[[l]][2])
  dim[l]=bigres[[l]][2]
}

datafr=data.frame(theta,dim)

write.table(datafr, file=dim_fn, sep=",", row.names=FALSE, col.names=FALSE) 














































