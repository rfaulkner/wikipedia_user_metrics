#
# Ryan Faulkner, September 25th 2012
#
# Editor Engagement Experiments
#
# Post Edit Feedback - Iteration #1 - Analysis
#

home_dir <- "/Users/rfaulkner/"
project_dir <- paste(home_dir, "projects/E3Analysis/", sep="")

helper_import <- paste(project_dir,"R/R_helper_functions.R",sep="")
viz_import <- paste(project_dir,"R/visualize_e3_data.R",sep="")


# FUNCTION :: pef1.bytes.added.process
#
# Code for PEF-1 analysis bytes added
#

pef1.bytes.added.process <- function(data, columns) {

	treatments <- c("control", "experimental_1", "experimental_2")

    new_data <- data
    non_log_data <- data

	for (i in columns)
	{
	    print(paste("Processing Metric",i,":",sep=" "))
	    data_lst <- c()

        for (j in treatments) {

            print(paste("Processing treatment",j,":",sep=" "))

            filter <- filter.list.by.regex(j, data$bucket)
            data_lst <- data[[i]][filter] / data[["edit_count"]][filter]

            if (i == "bytes_added_neg")
                data_lst <- abs(data_lst)

            data_lst <- data_lst[data_lst > 0]
            # data_lst <- add.small.constant(data_lst, 10^5)
            # data_lst <- log.transform(data_lst)

            non_log_data[[i]][filter.list.by.regex(j, new_data$bucket)] <- data_lst
            new_data[[i]][filter.list.by.regex(j, new_data$bucket)] <- log(data_lst)

            print(paste("Shapiro Test for", j, "under", i, sep=" "))
	          print(shapiro.test(log(data_lst)))
	          print(paste("Sample Size:",length(data_lst)))
	          print(paste("Mean:",mean(data_lst)))
	          print(paste("Log Mean:",mean(log(data_lst))))
	          print(paste("Log SD:",sd(log(data_lst))))
        }

        # Generate T-Tests

        ctrl <- new_data[[i]][filter.list.by.regex("control", new_data$bucket)]
        t1 <- new_data[[i]][filter.list.by.regex("experimental_1", new_data$bucket)]
        t2 <- new_data[[i]][filter.list.by.regex("experimental_2", new_data$bucket)]

        print(paste("T-test for treatment1", j, "under", i, sep=" "))
        print(t.test(x=t1, y= ctrl, alternative = "two.sided", paired = FALSE, var.equal = FALSE, conf.level = 0.95))

        print(paste("T-test for treatment1", j, "under", i, sep=" "))
        print(t.test(x=t2, y= ctrl, alternative = "two.sided", paired = FALSE, var.equal = FALSE, conf.level = 0.95))

        filename = paste("/Users/rfaulkner/projects/Analysis/Post-Edit-Feedback/i1/", "PEF1_", i, ".png", sep="")
        # df <- data.frame(Treatment=c(rep("Control", length(ctrl)),rep("Confirmation", length(t1)),rep("Gratitude", length(t2))), Byte_count_per_edit=c(ctrl,t1, t2),  se=c(rep(sd(ctrl), length(ctrl)),rep(sd(t1), length(t1)), rep(sd(t2), length(t2))))

        # BOXPLOT
        # p <- p + geom_boxplot(aes(fill = Treatment))
        # p <- ggplot(df, aes(Treatment, Byte_count_per_edit))
        # p <- p + geom_boxplot(aes(fill = Treatment))

        # ERRORBARS
        df <- data.frame(treatment=factor(c("Control", "Confirmation", "Gratitude")), bytes_added=c(mean(ctrl), mean(t1), mean(t2)), se=c(sd(ctrl), sd(t1), sd(t2)))
        p <- ggplot(df, aes(y=bytes_added, x=treatment))
        p <- p + theme_bw()+opts(panel.grid.major=theme_blank(),panel.grid.minor=theme_blank()) + scale_y_continuous(limits=c(0, 8))
        p <- p + geom_point() + geom_errorbar(aes(ymax = bytes_added + se, ymin = bytes_added - se), width=0.2)

        # SCATTERPLOT

        ggsave(filename)
	}

}


# FUNCTION :: pef1.bytes.added.process
#
# Code for PEF-1 analysis bytes added
#

pef1.time.to.threshold.process <- function(data) {

	treatments <- c("control", "experimental_1", "experimental_2")
    i <- "time_diff"
    data_frames <- c()

    for (j in treatments) {

        filter <- filter.list.by.regex(j, data$bucket) & data[[i]] > 0
        data_lst <- data[[i]][filter]
        df <- data.frame(x=log(data_lst))
        data_frames <- c(data_frames, df)

        filename = paste("/Users/rfaulkner/projects/Analysis/Post-Edit-Feedback/i1/PEF1_ttt_",j,"_hist.png",sep="")

        # Build Histogram
        p <- qplot(x, data=df, geom="histogram", binwidth=5)
        plot_title <- paste("Distribution of Time to Threshold(",j,")",sep="")
        p <- p + opts(title = plot_title)
        ggsave(filename)

        print(paste("Shapiro Test for", j, "under", i, sep=" "))
        print(shapiro.test(data_lst))
        print(paste("Sample Size:",length(data_lst)))
        print(paste("Mean:",mean(data_lst)))
        print(paste("SD:",sd(data_lst)))
    }

    # Generate T-Tests

    print(paste("T-test for treatment1", j, "under", i, sep=" "))
    print(t.test(x=data_frames[1]$x, y=data_frames[2]$x, alternative = "two.sided", paired = FALSE, var.equal = FALSE, conf.level = 0.95))

    print(paste("T-test for treatment1", j, "under", i, sep=" "))
    print(t.test(x=data_frames[1]$x, y=data_frames[3]$x, alternative = "two.sided", paired = FALSE, var.equal = FALSE, conf.level = 0.95))
}