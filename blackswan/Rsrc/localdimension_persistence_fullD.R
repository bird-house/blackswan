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

# TODO: must be detected or pass through the arguments
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

## open netCDF
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

#Quantile definition
quanti=0.98
npoints=nrow(dat)
dim=numeric(npoints)
theta=numeric(npoints)

ptm <- proc.time()
dist=pdist2(dat,dat)

# For CMIP5 models distance between point with itself is not 0!, but some small positive value
dist[dist<1] <- 0
print(proc.time() - ptm)

for (l in 1:npoints){

  distance=dist[,l]
  logdista=-log(distance)

  # Compute the thheshold corresponding to the quantile
  # remove INF
  logdista = logdista*is.finite(logdista)
  thresh=quantile(logdista, quanti, type = 5, na.rm=TRUE)

  #New Computation of theta
  Li<-which(logdista>thresh)

  #Length of each cluster
  Ti<-diff(Li)
  N=length(Ti)

  q=1-quanti
  Si=Ti-1

  Nc=length(which(Si>0))
  N=length(Ti)
  theta[l]=(sum(q*Si)+N+Nc-sqrt(((sum(q*Si)+N+Nc)^2)-8*Nc*sum(q*Si)))/(2*sum(q*Si))

  #Sort the exceedances
  logdista=sort(logdista)

  #Find all the Peaks over Thresholds. Question, why over threshold?
  findidx=which(logdista>thresh)

  #logextr=logdista[findidx[[1]]:(length(logdista)-1)]
  # -1 for INF!!!
  logextr=logdista[findidx[[1]]:(length(logdista))]

  #The inverse of the dimension is just the average of the exceedances, why?
  dim[l]=1/mean(logextr-thresh)
  #print(dim[l])
  #print(theta[l])
  
  l=l+1
  #print(l)
}

datafr=data.frame(theta,dim)

write.table(datafr, file=dim_fn, sep=",", row.names=FALSE, col.names=FALSE) 














































