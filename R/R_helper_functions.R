# Ryan Faulkner, January 26th 2012
#
# Set of helper functions for use in R
#  



# FUNCTION
#
# Given a set of data compute a normal distribution and the probabilities of falling on each bin
#
# 	bins --
# 	data --
#

get_normal_bins <- function(bins, data) {

	sample_sd <- sd(data)
	sample_mean <- mean(data)
	df <- length(data) - 1
	
	# vector to store bucket probabilities	
	probs <- c()
	num_bins <- length(bins)
	
	# Compute the probabilities
	
	for (i in 1:num_bins) 
	{
		if (i == 1) {
			upper <- bins[1] + ((bins[2] - bins[1]) / 2)
			lower <- bins[1] - ((bins[2] - bins[1]) / 2)
		} else if (i == num_bins) {
			upper <- bins[num_bins] + ((bins[num_bins] - bins[num_bins-1]) / 2)
			lower <- bins[num_bins] - ((bins[num_bins] - bins[num_bins-1]) / 2)
		} else {
			ip1 <- i + 1
			im1 <- i - 1
			upper <- bins[i] + ((bins[ip1] - bins[i]) / 2)
			lower <- bins[i] - ((bins[i] - bins[im1]) / 2)
		}

		# p = pnorm(upper, mean = sample_mean, sd = sample_sd, log = FALSE) - pnorm(lower, mean = sample_mean, sd = sample_sd, log = FALSE)
		p = pt(upper - sample_mean, df) - pnorm(lower - sample_mean, df)
		probs <- c(probs, p)
	}
	
	probs <- probs / sum(probs) 	# normalize the probabilities
	probs
}



# FUNCTION
#
#
# Given a set of data compute a normal distribution and the probabilities of falling on each bin
#
# 	bins --
# 	value --
#

find_bin <- function(bins, value) {
	distances <- abs(bins - value) 
	index <- order(sapply(distances, min))[1]
	bins[index]
}


# FUNCTION :: get_bin_counts
#
# Given a set of data break it into bins and return the counts with the bin index
#

get_bin_counts <- function(bins, data) {
	
	new_data <- c()
	for (i in 1:length(data))		
	{
		bin <- find_bin(bins, data[i])
		new_data <- c(new_data, bin)
	}
		
	tab <- table(new_data)
	xu <- as.numeric(names(tab))
	xn <- as.vector(tab)
	data.frame(values=xu, counts=xn)
}


# FUNCTION :: construct_probs
#
# Extract the probabilities corresponding to the samples
#

construct_probs <- function(values, full_probs) {
	
	sample_probs <- c()
	
	for (i in 1:length(values))
	{
		val <- values[i]
		bin <- find_bin(full_probs$values, val)
		index <- which(full_probs$values == bin)[1]
		sample_probs <- c(sample_probs, full_probs$counts[index])
	}
	
	sample_probs
}


# FUNCTION :: convert_to_bins
#
# Maps counts from a data frame (values, counts) to a pre-defined set of bins
#

convert_to_bins <- function(bins, samples) {
	
	for (i in 1:length(samples$values))
		samples$values[i] <- find_bin(bins, samples$values[i])
	
	samples
}


# FUNCTION :: pad_counts
#
# Pad counts from a data frame (values, counts) in a given range to contain 0 values where a bin is missing
#

pad_counts <- function(bin_range, samples) {

	new_values <- c()
	new_counts <- c()
		
	for (i in bin_range)
	{
		if (i %in% samples$values)
		{
			index <- which(samples$values == i)[1]
			new_values <- c(new_values, i)
			new_counts <- c(new_counts, samples$counts[index])

		} else {
			new_values <- c(new_values, i)
			new_counts <- c(new_counts, 0)
		}
	}
	
	data.frame(values=new_values, counts=new_counts)
}


# FUNCTION :: append.data.frames
#
# Given two data frames append the second to the first
#
# Assumes: the two data frames have the same column names
#

append.data.frames <- function(df_1, df_2, string_frames=c(0)) {
	
	df_cols <- length(colnames(df_1))
	df_rows_1 <- length(df_1[[1]])
	df_rows_2 <- length(df_2[[1]])
	
	new_rows <- df_rows_1 + df_rows_2
	df_return <- data.frame(matrix(nrow=new_rows, ncol=df_cols))

	# Set the column names
	for (i in 1:df_cols)
	{
	    df_return[[i]] <- c(df_1[[i]], df_2[[i]])

		colname <- colnames(df_1)[i]
		colnames(df_return)[i] <- colname
	}
	
	df_return
}


# FUNCTION :: build.data.frames
#
# Constructs a concatenated data.frame from files
#

build.data.frames <- function(template_indices, fname_first_part = "output/metrics_z",
                            fname_last_part = "_editcounts.tsv",
                            home_dir = "/Users/rfaulkner/projects/wsor/message_templates/", string_frames=c(0),
                            show_output = TRUE) {
	
	# Initialize the data frame
	
	filename <- paste(home_dir, fname_first_part, template_indices[1], fname_last_part, sep="")
	metrics = read.table(filename, na.strings="\\N", sep="\t", comment.char="", quote="", header=T)

	if (show_output) {
        output <- paste("Processing data from",filename,"....")
        print(output)
	}

	# Extend the data frames

	if (length(template_indices) > 1)
		for (i in 2:length(template_indices))
		{									
			index <- template_indices[i]		
			
			# Make an exception for the "0" template
			if (index == 0)
				filename <- paste(home_dir, "output/metrics_z", index, fname_last_part, sep="")	
			else
				filename <- paste(home_dir, fname_first_part, index, fname_last_part, sep="")	

			if (show_output) {
			    output <- paste("Processing data from",filename,"....")
			    print(output)
			}

			temp_frame = read.table(filename, na.strings="\\N", sep="\t", comment.char="", quote="", header=T)			
			metrics <- append.data.frames(metrics, temp_frame, string_frames=string_frames)		
		}
	
	metrics
}


# FUNCTION :: get.decrease.in.edits.after.template
#
# Huggle/Twinkle Experiment metrics --  Build list of edit decrease after postings
#

get.decrease.in.edits.after.template <- function(revisions_before, revisions_after, lower_bound_rev_before=0, lower_bound_rev_after=-1) {
	
	metrics <- c()
	
	for (i in 1:length(revisions_before)) 
		if (revisions_before[i] > lower_bound_rev_before & revisions_after[i] > lower_bound_rev_after)
			metrics <- c(metrics, 
			(revisions_before[i] - revisions_after[i]) / revisions_before[i])
			
	metrics
}


# FUNCTION :: get.change.in.blocks
#
# Huggle/Twinkle Experiment metrics --  Build list of block decrease after postings
#

get.change.in.blocks <- function(blocks_before, blocks_after, lower_bound_block_before=0, lower_bound_block_after=0) {
	
	metrics <- c()
	
	for (i in 1:length(blocks_before)) 
		if (blocks_before[i] > lower_bound_block_before | blocks_after[i] > lower_bound_block_after)
			# metrics <- c(metrics, blocks_after[i] - blocks_before[i])
			
			if (blocks_after[i] > blocks_before[i])
				metrics <- c(metrics, 1)
			else
				metrics <- c(metrics, 0)
			
	metrics
}


# FUNCTION :: convert.list.to.binomial.event
#
# modify a list of non-negative integers to store binomial values indicating the presence or absence of an event 
#

convert.list.to.binomial.event <- function(value_list) {
	value_list[value_list > 0] = 1
	value_list[value_list == 0] = 0
	value_list
}


# FUNCTION :: filter.list.by.regex
#
# Take a list and filter out elements that don't match the pattern
#

filter.list.by.regex <- function(pattern, value_list) {	
	new_list <- c()	
	for (i in 1:length(value_list)) 
		if (length(grep(pattern, value_list[i], perl = TRUE)) > 0)
			new_list <- c(new_list, TRUE)
		else
			new_list <- c(new_list, FALSE)
	new_list
}


# FUNCTION :: counts.to.samples
#
# Take a list of counts and produce a new list of samples with the same distribution of those counts
#

counts.to.samples <- function(sample_values, counts) {
		
	samples <- c()
	
	for (i in sample_values)
	{
		samples_to_add <- i * (1:counts[i] / 1:counts[i])
		samples <- c(samples, samples_to_add)	
	}
	
	samples
}

# FUNCTION :: add.small.constant
#
# Shift elements in list by a small positive constant.  This removes all '0's from the list.
#
add.small.constant <- function(data, norm_factor) { data + min(abs(data[data != 0])) / norm_factor }
