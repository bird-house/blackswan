print(paste(Sys.getenv("CONDA_PREFIX"), "bin/unzip", sep = "/"))
options(unzip = paste(Sys.getenv("CONDA_PREFIX"),
		      "bin/unzip", sep = "/"))
print(getOption("unzip"))
print(list.files("/bin/tar"))
Sys.setenv(TAR = "/bin/tar")

library(ggplot2)
library(FARallnat)
library(FARg)
print("package loaded")

compute_and_plot_far <- function(mdata, yvar="y", xvar="x", tvar="time", xp=1.6 , R=3, stat_model=gauss_fit, ci_p=0.9, pdf_name = NULL,  ...){
  ans <- compute_far_simple(mdata,
                            y = yvar, x = xvar, time = tvar,
                            xp = xp, R  = R, ci_p = ci_p,
                            stat_model = stat_model, ...) 
  ellipsis <- list(...)
  ellipsis <- sapply(ellipsis, as.character)
  if(length(ellipsis) > 0 ){
    ellipsis <- paste(names(ellipsis), ellipsis, sep = "_", collapse = "_")
  } 
  else{
    ellipsis <- ""  
  }

  if(is.null(pdf_name)){
    pdf_name <- paste("FAR", "stat_model", deparse(substitute(stat_model)), "y", yvar, "x", xvar, "xp", xp, "R", R,  ellipsis, sep = "_")
    pdf_name <- paste(pdf_name, "pdf", sep = ".")
  }
  pdf(file = pdf_name)
  
  oridat <- data.frame(year = mdata[, tvar],
                       x = mdata[, xvar],
                       y = mdata[, yvar])
  # print(head(oridat))
  merged_res <- ans$ic_far
  p_dx <- default_plot(subset(merged_res, param %in% c("x_all", "x_ant", "x_nat"))) +
    geom_hline(aes_q(yintercept=xp), linetype=2) + 
    geom_point(data = oridat, aes(x=year, y=x), color="black",size=0.9, alpha=0.7) 
  p_dx <- reorder_layer(p_dx)
  plot(p_dx)
  
  p_mu <- default_plot(subset(merged_res, param %in% c("mu_all", "mu_ant", "mu_nat", "threshold_all", "threshold_ant", "threshold_nat"))) +
    geom_hline(aes_q(yintercept=xp), linetype=2) + 
    geom_point(data = oridat, aes(x=year, y=y), color="black",size=0.9, alpha=1) 
  p_mu <- reorder_layer(p_mu)
  plot(p_mu)
  
  p_p <- default_plot(subset(merged_res, param %in% c("p_all", "p_ant", "p_nat"))) + facet_grid(param ~ ., scales = "free")+
    coord_trans(y="sqrt")
  plot(p_p)
  
  plot_far(merged_res, "al")
  dev.off()
  invisible(ans)
}
