## Diagnostics de temps de retour (ou proximite) des analogues calcules
## en temps continu
## Pascal Yiou (LSCE), Jan. 2015
## Se lance par:
## R CMD BATCH "--args ${fileanalo} ${probs.c} ${probs.n} $filout}" /home/users/yiou/RStat/A2C2/analogs_diags-prox.R
## Rscript analogs_diags-prox.R output.txt 0.7 0.3 analogs_RT.Rdat
SI=Sys.info()

ANAdir="."

library(parallel)
library(ncdf4)
ncpus=3
yr.now=as.integer(format(Sys.time(), "%Y")) # Annee en cours
date=as.character(as.Date(format(Sys.time(), "%Y-%m-%d"))-1)
#date=as.character(as.Date("2017-01-06",  "%Y-%m-%d"))
args=(commandArgs(TRUE))
print(args)
if(length(args)>0){
    filin=args[1]
    analogs=read.table(filin,header=TRUE)
    probs.c=as.numeric(args[2]) # Quantile sur les correlations
    probs.n=as.numeric(args[3]) # quantile sur les distances
    filout = args[4]
}else{
    analogs=read.table(paste(ANAdir,"/output.txt",sep=""), header=TRUE)
    probs.c=0.7
    probs.n=0.3
    filout=paste("analogs_RT.Rdat")
}
probs.d=0.4

print(probs.c)

l.seas=list(DJF=c(12,1,2),MAM=c(3,4,5),JJA=c(6,7,8),SON=c(9,10,11))
month.seas=list(DJF=c(1,32,62),MAM=c(1,32,61),JJA=c(1,31,62),SON=c(1,31,62))
label.seas=list(DJF=c("DEC","JAN","FEB"),MAM=c("MAR","APR","MAY"),
  JJA=c("JUN","JUL","AUG"),SON=c("SEP","OCT","NOV"))

## Lecture du fichier d'analogues
##analogs=read.table(paste(NCEPdir,"slpano-NA.analog30rms.1d.all1948.dat",
##  sep=""),header=TRUE)

mm=floor(analogs$date/100) %% 100
mmdd=analogs$date %% 10000

ana.dates=analogs$date

## Calcul d'un cycle saisonnier des rms
rms.mean=apply(analogs[,22:41],1,mean)
rms.mean.ann=tapply(rms.mean,mmdd,mean)
rms.dum=rep(rms.mean.ann,times=3)
rms.smo=smooth.spline(rms.dum,spar=0.6)$y[367:732]
## Normalisation des distances par un cycle saisonnier
rms.norm=analogs[,22:41]
ummdd=unique(mmdd)
for(ii in ummdd){
  dd=which(mmdd==ii)
  jj=which(ummdd==ii)
  rms.norm[dd,]=rms.norm[dd,]/rms.smo[jj]
}

##thresh.c=0.6
## Seuils sur les quantiles pour definir les bons analogues
thresh.c=quantile(unlist(analogs[,42:61]),probs=probs.c)
thresh.d=quantile(unlist(analogs[,22:41]),probs=probs.d)
thresh.n=quantile(unlist(rms.norm),probs=probs.n)

Idat=1:nrow(analogs)

## Wrapper pour le calcul des RP
"index.analo" = function(i)
  {
    I=match(analogs[i,2:21],analogs$date)
    diff.i=abs(I-i)/365.25 ## Nombre d'annees de difference
    diff.cond=ifelse(rms.norm[i,] <= thresh.n &
      analogs[i,42:61] >= thresh.c, diff.i,NA)
    q.diff=quantile(diff.cond,probs=c(0.25,0.5,0.75),na.rm=TRUE)
    n.OK=length(which(!is.na(diff.cond)))
    return(c(q.diff,n.OK))
  }

## Calcul des temps de retours pour de bons analogues
I.dum=mclapply(Idat,index.analo,mc.cores=ncpus)

Index.tot=t(matrix(unlist(I.dum),nrow=4))

date.ref=20141201

## Determination de la date de reference et de la saison
## en fonction de la derniere
## date observee dans les reanalyses
date.max=max(analogs$date)
mm.max=floor(date.max/100) %% 100
if(mm.max %in% c(1,2)) {
  date.ref=(yr.now-1)*10000+1201
  seas="DJF"
}
if(mm.max == 12){
  date.ref=(yr.now)*10000+1201
  seas="DJF"
}
if(mm.max %in% c(3:5)){
  date.ref=(yr.now)*10000+0301
  seas="MAM"
}
if(mm.max %in% c(6:8)){
  date.ref=(yr.now)*10000+0601
  seas="JJA"
}
if(mm.max %in% c(9:11)){
  date.ref=(yr.now)*10000+0901
  seas="SON"
}
# refdate will be 2-3 months ahead...
if (mm.max <= 2) {
 date.ref=(yr.now-1)*10000+1000+mm.max*100+1
} else {
 date.ref=(yr.now)*10000+(mm.max-2)*100+1
}

yyyy=floor(analogs$date/10000)
uyyyy=unique(yyyy)
yyyy[mm==12]=yyyy[mm==12]+1
I.seas=which(mm %in% l.seas[[seas]])
Index.tot.seas=tapply(Index.tot[I.seas,2],yyyy[I.seas],median,na.rm=TRUE)

setwd(ANAdir)
save(file=filout,Index.tot,ana.dates,analogs,rms.norm,thresh.c,thresh.n)

#q("no")

## Graphiques Nombre de bons analogues & RP
setwd(ANAdir)
##pdf("analogs_RP-diags.pdf")
require(ggplot2)
require(grid)
dfga <- data.frame(date=as.Date(as.character(analogs$date[analogs$date>=date.ref]), 
                                "%Y%m%d"), 
                   goodana=Index.tot[analogs$date>=date.ref,4])
dfrp <- data.frame(date=as.Date(as.character(analogs$date[analogs$date>=date.ref]), 
                                "%Y%m%d"),
                           rp=Index.tot[analogs$date>=date.ref,2], 
                   rpmin=Index.tot[analogs$date>=date.ref,1],
                   rpmax=Index.tot[analogs$date>=date.ref,3])

p <- ggplot(dfga,aes(x=date, y=goodana))
p <- (p+theme_bw() + geom_bar(stat="identity", width=0.7)
      + scale_x_date()
      + labs(y="number of good analogs", x=""))
print(p)
l <- ggplot(dfrp,aes(x=date))
l <- (l+theme_bw()
      + geom_ribbon(aes(ymin=rpmin,ymax=rpmax), fill = "grey70")
      + geom_line(aes(y=rp))
      + scale_x_date()
      + labs(y="Return time [yr]", x=""))
print(l)
png("analogs_RP-diags_new.png") #,bg="transparent")
grid.newpage()
vpp_ <- viewport(width = 1, height = 0.5, x = 0.5, y = 0.75)  # upper
vpl_ <- viewport(width = 1, height = 0.5, x = 0.5, y = 0.25)  # lower
print(p, vp = vpp_)
print(l, vp = vpl_)
dev.off()

r1=range(analogs[,22:41])
r2=range(analogs[,42:61])
r3=range(rms.norm)
dfrms <- data.frame()
for (i in 1:20 ){ 
  dfrms <- rbind(dfrms,data.frame(date=as.Date(as.character(analogs$date[analogs$date>=date.ref]), 
                                               "%Y%m%d"),
                                  rms=analogs[analogs$date>=date.ref,21+i]))
}
dfnrms <- data.frame()
for (i in 1:20 ){ 
  dfnrms <- rbind(dfnrms,data.frame(date=as.Date(as.character(analogs$date[analogs$date>=date.ref]), 
                                               "%Y%m%d"),
                                  nrms=rms.norm[analogs$date>=date.ref,i]))
}
dfcor <- data.frame()
for (i in 1:20 ){ 
  dfcor <- rbind(dfcor,data.frame(date=as.Date(as.character(analogs$date[analogs$date>=date.ref]), 
                                               "%Y%m%d"),
                                  cor=analogs[analogs$date>=date.ref,41+i]))
}
p <- ggplot(dfrms,aes(x=date, y=rms))
p <- (p+ theme_bw()
      + geom_point(size=1.0)
      + scale_x_date()
      + ylim(r1)
      + labs(x=""))
print(p)
q <- ggplot(dfnrms,aes(x=date, y=nrms))
q <- (q+ theme_bw()
      + geom_point(size=1.0)
      + scale_x_date()
      + ylim(r3)
      + labs(x="",y="Normalized rms")) 
print(q)
l <- ggplot(dfcor,aes(x=date, y=cor))
l <- (l+ theme_bw()
      + geom_point(size=1.0)
      + scale_x_date()
      + ylim(r2)
      + labs(x="",y="Correlation")) 
print(l)
png("analogs_score-diags_new.png")
grid.newpage()
vpp_ <- viewport(width = 1, height = 0.33, x = 0.5, y = 0.83)  # upper
vpq_ <- viewport(width = 1, height = 0.33, x = 0.5, y = 0.5 ) # middle
vpl_ <- viewport(width = 1, height = 0.33, x = 0.5, y = 0.17)  # lower
print(p, vp = vpp_)
print(q, vp = vpq_)
print(l, vp = vpl_)
dev.off()
## Scores d'analogues (rms et correlation)
##pdf("analogs_score-diags.pdf")
q("no")
