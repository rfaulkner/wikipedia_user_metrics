
"""
    Implements a feed-forward neural network
"""

__author__ = "Ryan Faulkner <rfaulkner@wikimedia.org>"
__date__ = "November 27th, 2012"
__license__ = "GPL (version 2 or later)"

from pybrain.datasets            import ClassificationDataSet
from pybrain.utilities           import percentError
from pybrain.tools.shortcuts     import buildNetwork
from pybrain.supervised.trainers import BackpropTrainer
from pybrain.structure.modules   import SoftmaxLayer


import src.data_representation.aft_feedback as aft
import random

def read_data(data_factory):
    if hasattr(data_factory, '__iter__') and hasattr(data_factory, 'fields'):
    # ensure that the factory interface exists
        data = list()
        for k in data_factory.__iter__():
            data.append(k[:]) # unpack the contents
        return data


def train_network(trndata, tstdata, num_epochs=1000, log_frequency=100):
    fnn = buildNetwork( trndata.indim, 100, trndata.outdim, outclass=SoftmaxLayer )
    trainer = BackpropTrainer( fnn, dataset=trndata, momentum=0.01, verbose=True, weightdecay=0)

    for i in xrange(num_epochs):
        trainer.trainEpochs( 1 )
        trnresult = percentError( trainer.testOnClassData(),
            trndata['class'] )
        tstresult = percentError( trainer.testOnClassData(
            dataset=tstdata ), tstdata['class'] )

        if i % log_frequency == 0:
            print "epoch: %4d" % trainer.totalepochs,\
            "  train error: %5.2f%%" % trnresult,\
            "  test error: %5.2f%%" % tstresult

num_classes = 3
num_features = 5

alldata = ClassificationDataSet(num_features, 1, nb_classes=num_classes)
data = read_data(aft.AFTFeedbackFactory())
n = 1.0 / num_classes
l = list()
for k in xrange(num_classes):  l.append(n*k)

for k in data:
    r = random.random()
    klass = int(sum(map(lambda x: x >= r, l)))
    alldata.addSample(k, [klass])

tstdata, trndata = alldata.splitWithProportion( 0.25 )

trndata._convertToOneOfMany( )
tstdata._convertToOneOfMany( )

train_network(trndata, tstdata)



