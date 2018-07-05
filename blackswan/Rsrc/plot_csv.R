# Script to plot local dims vs theta
# Example adapted from: 
# ADP (https://stats.stackexchange.com/users/17602/adp), 
# Scatterplot with contour/heat overlay, 
# URL (version: 2015-08-20): https://stats.stackexchange.com/q/168049

# Arguments:
# Rscript plot_csv.R csvfilename ouputplotname.pdf

rm(list=ls())
library(MASS)
library(ggplot2)

args=(commandArgs(TRUE))

fname=args[1]

if(length(args)>1){
    foutname=args[2]
}else{
    foutname="local_dims.pdf"
}


csvdat <- read.table(file=fname, header=FALSE, sep=",")

# It's assumed that txt file has structure: time,theta,dim
df=data.frame(csvdat[3],csvdat[2]); colnames(df) = c("x","y")

#df=data.frame(dim,theta); colnames(df) = c("x","y")

commonTheme = list(labs(color="Density",fill="Density",
                        x="Local dimension",
                        y="Persistence"),
                   theme_bw(),
                   theme(legend.position=c(0,1),
                         legend.justification=c(0,1)))

#myPlot<-ggplot(data=df,aes(x,y)) + 
#  stat_density2d(aes(fill=..level..,alpha=..level..),geom='polygon',colour='black') + 
#  scale_fill_continuous(low="green",high="red") +
#  geom_smooth(method=lm,linetype=2,colour="red",se=F) + 
#  guides(alpha="none") +
#  geom_point() + commonTheme

myPlot<-ggplot(data=df,aes(x,y)) + 
  geom_point() +
  stat_density2d(aes(fill=..level..,alpha=..level..),geom='polygon',colour='black') + 
  scale_fill_continuous(low="green",high="red") +
  guides(alpha="none") +
  commonTheme

ggsave(filename=foutname, plot=myPlot, dpi=300, width = 35, height = 20, units = "cm")
