"""
    This module is used to define the reporting methodologies on different types of data.  The base class
    DataReporting is defined to outline the general functionality of the reporting architecture and 
    functionality which includes generating the data via a dataloader object and transforming the data
    among different reporting mediums including matlab plots (primary medium) and html tables.
    
    The DataLoader class decouples the data access of the reports using the Adapter structural pattern.
    
    The DataReporter functions under the following assumptions about the data format:
       
       -> each set of data points has a label
       -> labels in counts and times match
       -> all time labels match
       -> time data is sequential and relative (typically denoting minutes from a reference time)
       
        e.g.
        _counts_ = {'artifact_1' : [d1, d2, d3], 'artifact_2' : [d4, d5, d6]}
        _times_ = {'artifact_1' : [t1, t2, t3], 'artifact_2' : [t1, t2, t3]}

"""

__author__ = "Ryan Faulkner"
__revision__ = "$Revision$"
__date__ = "December 16th, 2010"


# Import python base modules """
import matplotlib
matplotlib.use('Agg') # disable plotting in the backend
import sys, logging, matplotlib.pyplot as plt, re


# Import Analytics modules """
import classes.TimestampProcessor as TP
import classes.DataLoader as DL
import classes.DataFilters as DF


# CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

standard_metric_names = {}


class DataReporting(object):
    """
        Base class for reporting fundraiser analytics.  The DataReporting classes are meant to handle DataLoader objects that pull data from MySQL
        and format the dictionaries for reporting in the form of plotting and tables.

    """

    class KwargDefError(Exception):
        """
            Exception class to handle missing query type on ConfidenceReporting obj creation
         """
        
        def __init__(self, value):
            self.value = value
        
        def __str__(self):
            return repr(self.value)


    def __init__(self, **kwargs):
        """
            Configure tunable reporting params

            Parameters:
                - **SQL_statement**: string variable storing the SQL query

            Return:
                - List(tuple).  The query results (or -1 for a failed query).
        """
        self._use_labels_= False
        self._fig_file_format_ = 'png'
        self._plot_type_ = 'line'
        self._item_keys_ = list()
        self._file_path_ = './'
        self._generate_plot_ = True

        self._font_size_ = 24
        self._fig_width_pt_ = 246.0                  # Get this from LaTeX using \showthe\columnwidth
        self._inches_per_pt_ = 1.0/72.27             # Convert pt to inch
    

        # CLASS MEMBERS: Store the results of a query
        self.data_loader_ = None
        self._table_html_ = ''       # Stores the table html
        self._data_plot_ = None      # Stores the plot object

        self._counts_ = dict()
        self._times_ = dict()
            
        self._set_filters()
        
        for key in kwargs:
            
            if key == 'font_size':
                self._font_size_ = kwargs[key]
            elif key == 'fig_width_pt':
                self._fig_width_pt_ = kwargs[key]
            elif key == 'inches_per_pt':
                self._inches_per_pt_ = kwargs[key]
            elif key == 'use_labels':
                self._use_labels_ = kwargs[key]
            elif key == 'fig_file_format':
                self._fig_file_format_ = kwargs[key]
            elif key == 'plot_type':                
                self._plot_type_ = kwargs[key]
            elif key == 'item_keys':                
                self._item_keys_ = kwargs[key]
            elif key == 'file_path':                
                self._file_path_ = kwargs[key]
            elif key == 'generate_plot':                
                self._generate_plot_ = kwargs[key]
            
        

    def _set_filters(self):
        """
            Initialize the filters for data post processing

            Return:
                - empty.
        """
        logging.info('Initializing filters.')
        
        self._filters_ = list()
        self._filters_.append(DF.TotalCountFilter(lower_bound=-1,mutable_obj=self))
        self._filters_.append(DF.MatchKeysDataReporterFilter(mutable_obj=self))


    def add_filters_runtime(self, **kwargs):
        """
            Add new filters at run time - ensure there are no duplicates in the list

            Parameters (\*\*kwargs):
                - **time_series**:
                - **artifact_keys**:
                - **interval**:

            Return:
                - empty.
        """

        #  Check Filter list
        contains_timeseries_filter = False
        for filter_inst in self._filters_:
            if isinstance(filter_inst, DF.TimeSeriesDataFilter):
                contains_timeseries_filter = True

        if kwargs['time_series'] and not(contains_timeseries_filter):    
            
            artifact_keys_var = kwargs['artifact_keys']
            interval_var = kwargs['interval']
                        
            self._filters_.append(DF.TimeSeriesDataFilter(artifact_keys=artifact_keys_var, interval=interval_var, mutable_obj=self))



    def _execute_filters(self):
        """
            Runs through he list of data reporting filters and executes each

            Return:
                - empty.
        """
        
        for filter_inst in self._filters_:
            filter_inst.execute()
        

    def get_counts(self):
        """
            These methods expose the reporting data.

            Return:
                - empty.
        """

        return self._counts_
    
    def get_times(self):
        return self._times_
    
    def set_counts(self, new_dict):
        self._counts_ = new_dict
    
    def set_times(self, new_dict):
        self._times_ = new_dict

    def _gen_plot(self, x, y_lists, labels, title, xlabel, ylabel, subplot_index, fname):

        """
            Private Method.  To be overloaded by subclasses for different plotting behaviour

            Parameters:

            Return:

        """

        return 0


    def _write_html_table(self, data, column_names, **kwargs):
        """
            Private method.  General method for constructing html tables.  May be overloaded by subclasses for writing tables - this functionality
            currently exists outside of this class structure (test_reporting.py)

            Parameters:

            Return:

        """

        # PROCESS KWARGS
        column_colours = dict()
        column_colours_idx = dict()
        
        #  In the case that the markup was formatted before this method was called modify the flag
        if 'omit_cell_markup' in kwargs.keys():
            omit_cell_markup = kwargs['omit_cell_markup']
        else:
            omit_cell_markup = False
            
        if 'use_standard_metric_names' in kwargs.keys():
            column_names = self.get_standard_metrics_list(column_names)        
        
        if 'coloured_columns' in kwargs.keys():
                        
            column_colours = kwargs['coloured_columns']            
                            
            # Assume kwargs['coloured_columns'] is a dictionary
            try:
                
                # Map column names if standard names are used
                if 'use_standard_metric_names' in kwargs.keys():
                    new_column_colours = dict()
                    for col_name in column_colours:
                        new_column_colours[standard_metric_names[col_name]] = column_colours[col_name]
                    column_colours = new_column_colours
                
                for col_name in column_colours:                    
                    column_colours_idx[column_names.index(col_name)] = column_colours[col_name]
                    
            except:
                column_colours = {}
                logging.error('Could not properly process column colouring.')
                pass

                    
        html = '<table border=\"1\" cellpadding=\"10\">'
        
        # Build headers
        if len(column_names) > 0:
            html = html + '<tr>'            
            
            for name in column_names:
                
                if name in column_colours:
                    html = html + '<th style="background-color:' + column_colours[name] + ';">' + name + '</th>'
                else:
                    html = html + '<th>' + name.__str__() + '</th>'
            html = html + '</tr>'
        
        # Build rows
        for row in data:            
            html = html + self._write_html_table_row(row, coloured_columns=column_colours_idx, omit_cell_markup=omit_cell_markup)
                 
        html = html + '</table>'        
        
        return html
    

    def _write_html_table_row(self, row, **kwargs):
        """
            Compose a single table row - used by _write_html_table

            Parameters:

            Return:
        """
        #    In the case that the markup was formatted before this method was called modify the flag
        if 'omit_cell_markup' in kwargs.keys():
            omit_cell_markup = kwargs['omit_cell_markup']
        else:
            omit_cell_markup = False
            
            
        if 'coloured_columns' in kwargs.keys():
            column_colours_idx = kwargs['coloured_columns']
        else:
            column_colours_idx = {}

        html = '<tr>'
        idx = 0 

        for item in row: 
              
            if omit_cell_markup:
                html = html + item.__str__()
            elif idx in column_colours_idx:
                html = html + '<td style="background-color:' + column_colours_idx[idx] + ';">' + item.__str__() + '</td>'
            else:
                html = html + '<td>' + item.__str__() + '</td>'
            
            idx = idx + 1
        
        html = html + '</tr>'
        
        return html
    
    

    def write_html_table_from_rowlists(self, data, column_names, key_type):
        """
            Enable table generation from data formatted a list of rows.

            Parameters:
                - **data**: .
                - **column_names**: .
                - **key_type**: .

            Return:
                - String.  Formatted HTML
        """

        html = '<table border=\"1\" cellpadding=\"10\"><tr>'
        
        # mapped data stores autovivification structure as a list of rows
        
        # Build headers
        html = html + '<th>' + key_type + '</th>'
        for name in column_names:
            html = html + '<th>' + name + '</th>'
        html = html + '</tr>'
        
        # Build rows
        for row in data:
            html = html + '<tr>'
            for item in row:                                    
                html = html + '<td>' + item + '</td>'
            html = html + '</tr>'
        
        html = html + '</table>'        
        
        return html



    def get_data_lists(self, patterns, empty_data):
        """
            Helper method that formats Reporting data (for consumption by javascript in live_results/index.html)

            Parameters:
                - **pattern** - List(String). A set of regexp patterns on which data keys are matched to filter
                - **empty_data** - List. A set of empty data to be used in case there is no usable data from the reporting object

            Return:
                - Dictionary.  Stores data for javascript processing

        """

        # Get metrics
        data = list()
        labels = '!'
        counts = list()
        max_data = 0.0
        min_data = 0.0

        
        # Only add keys with enough counts
        data_index = 0
        for key in self._counts_.keys():
            
            isFormed = False
            for pattern in patterns:
                if key == None:
                    isFormed = isFormed or re.search(pattern, '')
                else:
                    isFormed = isFormed or re.search(pattern, key)
                    
            # if sum(self._counts_[key]) > 0.01 * max and isFormed:
            if isFormed:
                data.append(list())
                
                if key == None or key == '':
                    labels = labels + 'empty?'
                else:
                    labels = labels + key + '?'
                
                counts.append(len(self._counts_[key]))  
                
                for i in range(counts[data_index]):
                    data[data_index].append([self._times_[key][i], self._counts_[key][i]])
                    if self._counts_[key][i] > max_data:
                        max_data = self._counts_[key][i]
                    if self._counts_[key][i] < min_data:
                        min_data = self._counts_[key][i]
                        
                data_index = data_index + 1
            
        labels = labels + '!'
        
        # Use the default empty data if there is none
        if not data:
            return {'num_elems' : 1, 'counts' : [len(empty_data)], 'labels' : '!no_data?!', 'data' : empty_data, 'max_data' : 0.0, 'min_data' : 0.0}
        else:
            return {'num_elems' : data_index, 'counts' : counts, 'labels' : labels, 'data' : data, 'max_data' : max_data, 'min_data' : min_data}



class CategoryReporting(DataReporting):
    """

    """

    def __init__(self, **kwargs):
        """
            Constructor for CategoryReporting.

            Parameters:
                - **\*\*kwargs** - Dict(String). Plotting parameters.

            Return:
                - empty.
        """

        # Process kwargs
        for key in kwargs:
            if key == 'was_run':
                self._was_run_ = kwargs[key]
                
        # Initialize data loader objects
        self._PC_table_loader_ = DL.PageCategoryTableLoader()
        self._LP_table_loader_ = DL.LandingPageTableLoader()
        
        # Call constructor of parent
        DataReporting.__init__(self, **kwargs)
    

    def _gen_plot_bar(self, category_counts, title, fname):
        """
            Generates a bar plot of categories from banners

            Parameters:
                - **category_counts** - List(String). A set of regexp patterns on which data keys are matched to filter
                - **title** - String.
                - **fname** - String.

            Return:
                - empty.

        """

        # Add category data to a list from dict object
        data = list()
        for key in category_counts:
            data.append(category_counts[key])
        category_names = category_counts.keys()
        
        spacing = 0.5
        width = 0.3
        
        # Generate a histogram for each artifact
        subplot_index = 111

        colours = ['r', 'b', 'y', 'g']
        iter_colours = iter(colours)
        indices = range(len(category_names))
        
        rects = list()
        
        # Build the tick labels
        tick_pos = list()
        for i in indices:            
            tick_pos.append(spacing + width + i * spacing + i * width)
            
        plt.clf()
        
        self._font_size_ = 14
        params = {'axes.labelsize': self._font_size_,
          'text.fontsize': self._font_size_,
          'xtick.labelsize': self._font_size_,
          'ytick.labelsize': self._font_size_,
          'legend.pad': 0.1,     # empty space around the legend box
          'legend.fontsize': self._font_size_,
          'font.size': self._font_size_,
          'text.usetex': False,
          'figure.figsize': [26,14]}

        plt.rcParams.update(params)
        
        plt.subplot(subplot_index)
        plt.figure(num=None,figsize=[26,14])
        plt.xticks(tick_pos, category_names)
        plt.grid()
        plt.title(title)
        plt.ylabel('% CHANGE')
        
        bar_pos = list()
        for i in indices:
            bar_pos.append(spacing + i * spacing + i * width + width / 2)
        rects.append(plt.bar(bar_pos, data, width, color=iter_colours.next())[0])
        
        # plt.legend(rects, data.keys())
        plt.savefig(self._file_path_ + fname + '_bar.' + self._fig_file_format_, format=self._fig_file_format_)
    

    def _gen_plot_pie(self, category_counts, title, fname):
        """
            Generates a pie chart of categories from banners

            Parameters:
                - **category_counts** - .
                - **title** - String.
                - **fname** - String.

            Return:
                - empty.
        """

        # Add category data to a list from dict object
        data = list()
        category_names = list()
        for key in category_counts:
            data.append(category_counts[key])
            category_names.append(key)

        plt.clf()

        params = {'axes.labelsize': self._font_size_,
          'text.fontsize': self._font_size_,
          'xtick.labelsize': self._font_size_,
          'ytick.labelsize': self._font_size_,
          'legend.pad': 0.1,     # empty space around the legend box
          'legend.fontsize': self._font_size_,
          'font.size': self._font_size_,
          'text.usetex': False,
          'figure.figsize': [26,14]}

        plt.rcParams.update(params)
        
        plt.subplot(111)
        plt.figure(num=None,figsize=[26,14])
        plt.grid()
        plt.title(title)
        plt.pie(data, labels=category_names)
        
        plt.savefig(self._file_path_ + fname + '_pie.' + self._fig_file_format_, format=self._fig_file_format_)
        


    def run(self, start_time, end_time, campaign):
        """
            Execution method for category reporting.

            Parameters:
                - **start_time** - .
                - **end_time** - .
                - **campaign** - .

            Return:
        """

        timestamps = self._LP_table_loader_.get_log_start_times(start_time, end_time)
        
        start_time_formatted = TP.timestamp_convert_format(start_time, 1, 2) 
        end_time_formatted = TP.timestamp_convert_format(end_time, 1, 2)

        logging.info('Getting referred pages between %s and %s ...' % (start_time_formatted, end_time_formatted))
        page_ids = list()
        for ts in timestamps:                        
            page_ids.extend(self._LP_table_loader_.get_lp_referrers_by_log_start_timestamp(TP.timestamp_from_obj(ts,1,3), campaign)[0])
            
        logging.info('%s Referred pages ...' % str(len(page_ids)))
        # category_counts = self._PC_table_loader_.get_article_vector_counts(page_ids)
        category_counts = self._PC_table_loader_.get_normalized_category_counts(page_ids)            
        
        title = 'Histogram of Top Level Categories: %s - %s ' % (start_time_formatted, end_time_formatted)
        fname = 'referrer_categories_' + campaign
        
        self._gen_plot_bar(category_counts, title, fname)
        
        return category_counts
    
    