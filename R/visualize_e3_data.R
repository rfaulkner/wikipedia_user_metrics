
#
# Ryan Faulkner
# Wikimedia Foundation
# Editor Engagement Experimentation
#
# Create Date = February 28th 2012
#
# Data Visualization wrapper methods leveraging ggplots
#  

home_dir <- "/Users/rfaulkner/"
project_dir <- paste(home_dir, "projects/E3Analysis/", sep="")
helper_import <- paste(project_dir,"R/R_helper_functions.R",sep="")

source(helper_import)
library(ggplot2)



# FUNCTION :: line.plot.results
#
# Generates metrics for the test and control template sets and visualizes them in a line plot.  Assumes that the data exists, called from a
# "build.data.points*" method
#
#   data - formatted data frame containing:
#     
#     $x - independent variable (e.g. time series)
#     $y[1-n] - effects
#     $bucket[1-n] - buckets
#     $y[1-n]_[min|max] - bounds for error bars if included
#
#     e.g.
#       data = 
#        x y1 y2 bucket1 bucket2 y1_min y1_max y2_min y2_max
#        1 1  4  1 control    test    3.5    4.5      1      1
#        2 2  5  2 control    test    4.5    5.5      1      3
#        3 3  4  3 control    test    3.5    4.5      3      3
#

line.plot.results <- function(data, 
                              num_treatments, 
                              plot_width = 10, 
                              save_plot = FALSE, 
                              filename = 'line_plot.png', 
                              error_bars = FALSE,
                              plot_title = "Default Plot", 
                              x_scale = "x",
                              y_scale = "Sample Size", 
                              filedir = '/Users/rfaulkner/projects/E3Analysis/R/plots/')
{
		
	# df <- data.frame(x=1:length(means_test), y_test=means_test, y_ctrl=means_control, y_test_sd=sd_test, y_ctrl_sd=sd_control)	
  p <- ggplot(data,aes(x))
	
	for (j in 1:num_treatments) {
    
    bucket_attribute <- paste("bucket",j,sep="")
    effect_attribute <- paste("y",j,sep="")

    p <- p + geom_line(aes_string(y=effect_attribute,linetype=bucket_attribute))

    if (error_bars)
  	{
  	  effect_min <- paste("y",j,"_min",sep="")
  	  effect_max <- paste("y",j,"_max",sep="")
      p <- p + geom_errorbar(aes_string(ymin = effect_min, ymax = effect_max, linetype=bucket_attribute), width=0.2)
	  }
	}
       
	# Add axes labels and titles
	p <- p + opts(title = plot_title, legend.title = theme_blank()) + theme_bw() #,

  # axis.text.x = theme_text(colour = 'black'), axis.text.y = theme_text(colour = 'black'),
	# axis.title.x = theme_text(size = 14), axis.title.y = theme_text(size = 16, angle = 90), plot.title = theme_text(size = 18))

	if (save_plot)
		ggsave(paste(filedir,filename,'.png',sep=""), width=plot_width)
  else
    p
}


#
# FUNCTION :: plot.sample.sizes
#
#   data - formatted data frame containing:
#     
#     $x - independent variable (e.g. time series)
#     $y[1-n] - effects
#     $bucket[1-n] - buckets
#

plot.sample.sizes <- function(data, 
                              num_treatments,
                              plot_width = 10, 
                              save_plot = FALSE, 
                              filename = 'ggplot_out_', 
                              error_bars = FALSE,
                              plot_title = "Default Plot", 
                              x_scale = "x",
                              y_scale = "Sample Size", 
                              plot_title_metric = "Metric Description", 
                              filedir = '/Users/rfaulkner/projects/E3Analysis/R/plots/')
{

	# PLOT - Sample Sizes
	
  # Build bins, counts, and lables
  #
  #   bins - 
  #   counts -
  #   lables -
  
	bins <- 1:length(data$x)
	
  counts <- c()
  labels <- c()
  
	for (j in 1:length(num_treatments)) {
    
	  bucket_attribute <- paste("bucket",j,sep="")
	  effect_attribute <- paste("y",j,sep="")
	  
	  effect <- data[[effect_attribute]]
	  bucket <- data[[bucket_attribute]]
    
	  count_iter <- counts.to.samples(bins, data[[effect_attribute]])
	  counts <- c(counts, count_iter)
	  labels <- c(labels, rep(data[[bucket_attribute]], length(bins)))
	}
       	
	df <- data.frame(x=c(test_samples, control_samples), labels=labels) 
	p <- ggplot(df, aes(x, fill=labels)) + geom_bar(binwidth=0.4, position="dodge")
	p <- p + scale_x_continuous(x_scale) + scale_y_continuous('Sample Size') + opts(title = plot_title, legend.title = theme_blank())+ theme_bw()
	
	if (save_plot)
		ggsave(paste(filedir,filename,"samples_",reg_str,'.png',sep=""), width=plot_width)
  else
    p
}

# FUNCTION :: plot.distribution
#
# Plots the distribution of a set of events.
#
#   data - formatted data frame, $x the sample values
#     e.g. data <- data.frame(x=c(1,1,1,2,2,5,5,7,7,7,7,7,7,7,10))
#   binwidth - specifies the width of the value buckets

plot.distribution <- function(data, 
                              cust_binwidth=1,
                              plot_width=6, 
                              save_plot = FALSE, 
                              filename = 'plot_dist.png',
                              plot_title = "Default Plot", 
                              x_scale = "sample", 
                              y_scale = "frequency",
                              plot_title_metric = "Distribution", 
                              filedir = '/Users/rfaulkner/projects/E3Analysis/R/plots/')
{
  
  p <- qplot(x, data=data, geom="histogram", binwidth=cust_binwidth)
  p <- p + opts(title = plot_title) + scale_x_continuous(x_scale) + scale_y_continuous(y_scale) + theme_bw()
  
  if (save_plot)
    ggsave(paste(filedir, filename), width=plot_width)
  else
    p
  
}

# FUNCTION :: scatter.plot
#
# Generates a scatter plot over the given data points
# 
#   data - formatted data frame, containing the sample values $y, the dependent variable $x, the treatment $bucket
#     e.g. data <- data.frame(x=c(1,2,3,1,2,3),y=c(5,4,5,1,1,1),bucket=c("ctrl","ctrl","ctrl","test","test","test"))
#
scatter.plot <- function(data,
                         plot_width = 6, 
                         save_plot = FALSE, 
                         filename = 'plot_dist.png',
                         plot_title = "Default Scatter Plot", 
                         x_scale = "Dependent Var", 
                         y_scale = "Sample Value",
                         plot_title_metric = "Distribution", 
                         filedir = '/Users/rfaulkner/projects/E3Analysis/R/plots/')
{
  p <- ggplot(data, aes(x=x, y=y, color=bucket)) + geom_point(shape=1)
  p <- p + opts(title = plot_title) + scale_x_continuous(x_scale) + scale_y_continuous(y_scale) + theme_bw()
  
  if (save_plot)
    ggsave(paste(filedir, filename), width=plot_width)
  else
    p
}

