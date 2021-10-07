import sys

from dataapis.graphingAPIHelperFunctions import *

# ----API----
def graph(graphInfo, datasetInfo, saveLocation):
  if(graphInfo.graphType == "scatter" or graphInfo.graphType == "line" or graphInfo.graphType == "bar" ):
    graphPlot(graphInfo, datasetInfo,saveLocation)
  else:
    print("Illegal graphType:"+graphInfo.graphType)


# ----Data Structures----
class GraphInfo:
  def __init__(self, graphType, title="",xRange=None, yRange=None, fractional=False, xLabel="", yLabel="",xTickPosition = None, xTickAlias=None, yTickPosition = None, yTickAlias=None, xLogScaling=False, yLogScaling=False):
    self.graphType= graphType#
    self.xRange = xRange#
    self.yRange = yRange#
    self.fractional = fractional
    self.title = title#
    self.xLabel = xLabel#
    self.yLabel = yLabel#
    self.xTickPosition = xTickPosition
    self.yTickPosition = yTickPosition
    self.xTickAlias = xTickAlias
    self.yTickAlias = yTickAlias
    self.xLogScaling = xLogScaling#
    self.yLogScaling = yLogScaling#

class DatasetParameters:
    name = None
    xy = None#
    errorBarsHigh = None
    errorBarsLow = None
    color=None#
    def __init__(self, xy=[[],[]],errorBarsHigh=[],errorBarsLow=[], color=None,name =""):
      self.xy= xy
      self.errorBarsHigh = errorBarsHigh
      self.errorBarsLow = errorBarsLow
      self.color= color
      self.name = name

if __name__=="__main__":
   main()

