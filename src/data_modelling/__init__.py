
"""
    This module defines the interaction between data and data models.  The interface operates with a DataFactory type
     and a Model type.  These types expose general interfaces that will be used to train a model that may be used in
     classification or in more advanced ways, such as generative or predictive models.
"""

__author__ = "Ryan Faulkner <rfaulkner@wikimedia.org>"
__date__ = "January 2nd, 2013"
__license__ = "GPL (version 2 or later)"

class DataFactory(object):
    """ Defines the interface for a DataFactory """

    def __init__(self, **kwargs): raise NotImplementedError
    def __iter__(self, **kwargs): raise NotImplementedError
    def data_header(self): raise NotImplementedError
    def data_types(self): raise NotImplementedError

class Model(object):
    """ Defines the interface for a Model """

    def __init__(self, **kwargs): raise NotImplementedError
    def train(self, data_factory): raise NotImplementedError
    def get_params(self): raise NotImplementedError
    def classify(self, data_point): raise NotImplementedError

def train_model(data_factory, model, **kwargs):
    """ Module method that handles matching data with models """

    # get data
    df = data_factory(**kwargs)

    # train model conditioned on data
    m = model(**kwargs)
    m.train(df)

    # return model
    return m
