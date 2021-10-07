#Readme: Implements plotting APIs using VERSA's constructs (rmo, attributes) using python plotting libraries (matplotlib, seaborn, etc.).
import matplotlib
matplotlib.use('PDF') # Must be before importing matplotlib.pyplot or pylab!
import seaborn as sns
from mpl_toolkits.basemap import Basemap
import numpy as np
import matplotlib.animation as animation
import matplotlib.pyplot as plt
from geoalchemy2.shape import to_shape
from shapely.geometry import Polygon, mapping
from matplotlib.patches import Polygon as mplPolygon
#from itertools import chain, imap
import  collections
import dataapis.relational as re
import dataapis.export as ex
import dataapis.schema as sch
import pdb
import numpy as np
def initplot(plotcontext=None):
    sns.set()
    plt.title(plotcontext['title'])
    plt.ylabel(plotcontext['ylabel'])
    plt.xlabel(plotcontext['xlabel'])
# =======
#     fig, ax = sns.plt.subplots(1, 1)
#     sns.plt.title(plotcontext['title'])
#     sns.plt.ylabel(plotcontext['ylabel'])
#     sns.plt.xlabel(plotcontext['xlabel'])
# >>>>>>> 570358cd84f7dc032ee416c9c86b7d6d9eb2e749


def plot_rmo(session, rmo, xattr=None, yattr=None, label=None):
    #TODO use iterator to scan the dataset
    x_vals = []
    y_vals = []
    for res in ex.scan(session, rmo):
        x_vals.append(res._asdict()[xattr])
        y_vals.append(res._asdict()[yattr])
        plot = plt.plot(x_vals, y_vals,linewidth=0.3, label=label)


def plot_by_category(session=None, rmo=None, xattr=None, yattr=None, category_attr=None):
    agg_by_category = vapil.aggregate_array(session, rmo=rmo, arr_attrl=[x_attr, y_attr], agg_key=category_attr)

    #because postgres group by array and sorted do not work well together
    for rec in vapi.scan(session, agg_by_category):
            xVals = rec._asdict()['array_' + x_attr]
            yVals = rec._asdict()['array_'+ y_attr]

            zipped = zip(xVals, yVals)
            zipped.sort()
            xVals, yVals = zip(*zipped)
            cat = rec._asdict()[category_attr]
            plot = plt.plot(x_Vals, y_Vals,linewidth=0.3, label=cat)

def plot_data(x_vals=None, y_vals=None, label=None):
    plot = plt.plot(x_vals, y_vals,linewidth=0.3, label=label)
    plt.legend()


def plot_data_histogram_bins(values=None, weights = None, xl=None, xu=None, num_bins=None, line_plot_style=None, label=''):

    plt.yscale('log')
    if xl is None:
        xl = np.min(values)
    if xu is None:
        xu = np.max(values)

    bins = np.logspace(np.log10(xl), np.log10(xu), num_bins, base=10)

    color = 'b'
    ls ='-'
    marker=''
    if line_plot_style is not None:
        ls = line_plot_style[0]
        marker= line_plot_style[1]
        color = line_plot_style[3]

    #hist_vals, bin_edges = np.histogram(values, bins='auto', range=(0, 100))
    res = plt.hist(values, bins, weights=[w for w in weights], alpha=1.0, histtype='step', color=color, ls=ls, label=label)
    plt.gca().set_xscale("log")
    plt.gca().set_yscale("log")
    #sns.plt.savefig("out.png", dpi=1200)

# def plot_data_histogram_bins(values=None, weights = None, xl=None, xu=None, num_bins=None):
#     fig, ax = plt.subplots(1, 1)
#     plt.yscale('log')
#     plt.xscale('log')
#     if xl is None:
#         xl = np.min(values)
#     if xu is None:
#         xu = np.max(values)

#     #bins = np.linspace(xl, xu, num_bins)
#     bins = [np.log10(x) for x in np.logspace(np.log10(xl),np.log10(xu), num_bins)] #i don't understand np.logspace
#     res = plt.hist([np.log10(v) for v in values], bins, weights= weights, alpha=0.3, linewidth=0, color='r')
#     #freq = res[0]
#     #center = (bins[:-1]+bins[1:])/2
#     #width=(bins[1] - bins[0])/2
#     #opacity = 0.3

#     #sns.plt.bar(center, freq, align = 'center', width=width, alpha=opacity, linewidth=0, color='b')
#     #sns.plt.savefig(outfile)



def plot_scatter(session, rmo, xattr=None, yattr=None):
    x_vals = []
    y_vals = []
    for res in ex.scan(session, rmo):
        x_vals.append(int(res._asdict()[xattr]))
        y_vals.append(int(res._asdict()[yattr]))
    sns.regplot(np.asarray(x_vals),np.asarray(y_vals),fit_reg=False)


def saveplot(out_fn=None, plotcontext=None):
    if out_fn is None:
        out_fn  = plotcontext['title'] + ".png"
    plt.legend()
    plt.savefig(out_fn, dpi=1200)
    plt.clf()
    plt.close()

#------------------------------  MAP Graph --------------------------------


def createUSMap(resolution='l', figsize=(100,60)):
    #plt.figure(figsize=(100,60))
    #plt.figure(figsize=(300,180))
    plt.figure(figsize=figsize)
    map = Basemap(llcrnrlon=-119,llcrnrlat=22,urcrnrlon=-64,urcrnrlat=49,
            projection='lcc',lat_1=33,lat_2=45,lon_0=-95, resolution=resolution) #l for low resolution
    map.drawcoastlines(color="blue")
    map.drawcountries(color="blue")
    #map.drawmapboundary(fill_color='aqua')
    #map.fillcontinents(alpha=0.1)
    return map

def createIndiaMap(resolution='l', figsize=(100,60)):
    #plt.figure(figsize=(100,60))
    #plt.figure(figsize=(300,180))
    plt.figure(figsize=figsize)
    #map = Basemap(llcrnrlon=68.,llcrnrlat=6.,urcrnrlon=97,urcrnrlat=37,
    #        projection='lcc',lat_1=33,lat_2=45,lon_0=-4.36, resolution=resolution) #l for low resolution
    map = Basemap(width=1200000,height=900000,projection='lcc',resolution='l',
                    llcrnrlon=67,llcrnrlat=5,urcrnrlon=99,urcrnrlat=37,lat_0=28,lon_0=77)
    map.drawcoastlines(color="blue")
    map.drawcountries(color="blue")
    #map.drawmapboundary(fill_color='aqua')
    #map.fillcontinents(alpha=0.1)
    return map


def createMap( longitudeMin=None, longitudeMax=None, latitudeMin=None, latitudeMax=None,resolution = 'l'):

  """
  Creates a map object over an area given by the longitude axis.
  Resolution can be l for low, m for medium, and h for high.
  """
  longitudeSpan = longitudeMax - longitudeMin
  latitudeSpan = latitudeMax - latitudeMin
  print ("spans = ", longitudeSpan, " ", latitudeSpan)
  xsize = min(4096, max(20, longitudeSpan *  3))  #using 3 inches per long/lat span
  ysize = min(4096, (latitudeSpan * xsize)/longitudeSpan)
  print ("size=", xsize, " ", ysize)
  plt.figure(figsize=(xsize,ysize))
  map = Basemap(projection='merc', lat_0 = (latitudeMax+latitudeMin)/2, lon_0 = (longitudeMax + longitudeMin)/2,
                resolution = resolution, area_thresh = 0.1,
                llcrnrlon= longitudeMin, llcrnrlat=latitudeMin,
                urcrnrlon= longitudeMax, urcrnrlat=latitudeMax)

  map.drawcoastlines()
  map.drawcountries()
  #map.drawmapboundary(fill_color='aqua')
  #map.fillcontinents(color = 'coral',lake_color='aqua')
  map.drawmapboundary()
  return map


def add_coord_to_map(longitude, latitude, area_map):
    '''
    '''
    x,y = area_map(longitude, latitude)
    area_map.plot(x, y, 'bo', markersize=6)


    longitudeSpan = longitudeMax - longitudeMin
    latitudeSpan = latitudeMax - latitudeMin
    print("spans = ", longitudeSpan, " ", latitudeSpan)
    xsize = max(20, longitudeSpan *  3)  #using 3 inches per long/lat span
    ysize = (latitudeSpan * xsize)/longitudeSpan
    print("size=", xsize, " ", ysize)
    plt.figure(figsize=(xsize,ysize))
    map = Basemap(projection='merc', lat_0 = (latitudeMax+latitudeMin)/2, lon_0 = (longitudeMax + longitudeMin)/2,
    resolution = resolution, area_thresh = 0.1,
    llcrnrlon= longitudeMin, llcrnrlat=latitudeMin,
    urcrnrlon= longitudeMax, urcrnrlat=latitudeMax)

    map.drawcoastlines()
    map.drawcountries()
    #map.drawmapboundary(fill_color='aqua')
    #map.fillcontinents(color = 'coral',lake_color='aqua')
    map.drawmapboundary()
    return map




def addPointToMap(session, rmo, attr, area_map):
    """
    if the attribute (attr) in rmo (rmo) is a point, adds all points to the map (area_map)
    """
    attr_rmo = vapi.proj(session, rmo, [attr])
    rmo_iter = ex.build_iter(session, attr_rmo)

    def my_funct(ri,attr,area_map):
        wktShape=to_shape(getattr(ri, attr))

        point = mapping(wktShape)['coordinates']
        #TODO: figure out which is lat and which is long
        print("point= ", point[0]," ", point[1])
        x,y = area_map( point[0],point[1])

        modifiedMap = area_map.plot(x, y, 'bo', markersize=2)
    add_models_func = (lambda ri: my_funct(ri, attr, area_map))
    collections.deque(map(add_models_func, rmo_iter))
    return area_map


def addShapeToMap(session, rmo, attr, area_map):
    """
    if the attribute (attr) in rmo (rmo) is a non-point shape, adds all points to the map (area_map)
    Currently, multi-shapes (multilines/multipolygons) are treated as single objects
    """
    attr_rmo = sch.proj(session, rmo, [attr])
    rmo_iter = ex.build_iter(session, attr_rmo)

    def my_funct(ri,attr,area_map):
        wktShape=to_shape(getattr(ri, attr))
        area = mapping(wktShape)['coordinates']
        poly = mplPolygon([area_map(*x) for x in area[0][0]],facecolor='white',edgecolor='blue',linewidth=.5, alpha=.5, fill=False)
        plt.gca().add_patch(poly)
    add_models_func = (lambda ri: my_funct(ri, attr, area_map))
    collections.deque(map(add_models_func, rmo_iter))
    return area_map


def saveMap(fileLocation,dpi=256):
    #plt.tight_layout()
    plt.savefig(fileLocation,dpi=dpi)
    plt.clf()
    plt.close()



#------------------------------ Circos Graph -------------------------------------

#Example of using the matrix api
def exampleCircos():
    """
    Example for circos graph:

    title = "Friendship Connections"
    listOfNames = ["John", "James", "Jarven", "Jessica","Jimmy"]
    listOfConnections = [[0,0,0,3,0]
                        ,[0,0,0,1,0]
                        ,[0,0,0,0,2]
                        ,[1,1,0,0,1]
                        ,[0,0,1,0,0]]
    displayToScreen = True
    fileName = "foo.png"
    circosGraph(title, listOfNames, listOfConnections, displayToScreen, fileName)
    """
    title = "Friendship Connections"
    listOfNames = ["John", "James", "Jarven", "Jessica","Jimmy"]
    listOfConnections = [[0,0,0,3,0]
                        ,[0,0,0,1,0]
                        ,[0,0,0,0,2]
                        ,[1,1,0,0,1]
                        ,[0,0,1,0,0]]
    displayToScreen = True
    fileName = "foo.png"
    circosGraph(title, listOfNames, listOfConnections, displayToScreen, fileName)

def circosGraph(title, listOfLabels, connectionMatrix, displayToScreen, fileName):
    """
       Main API.
       Title - String that is displayed on top of the graph
       List of Labels - an array of the labels to be displayed on the graph
       listOfConnections - 2d array of weights, it needs to correspond with the labels
       display to screen - displays the graph to the screen
       fileName - the file name to sae the image at

       See exampleCircos for an example.
    """
    ax = __createCircosGraph(title, listOfLabels)
    for label1Index in range(len(connectionMatrix)):
        for label2Index in range(label1Index,len(connectionMatrix[label1Index])):
            __addCircosGraphConnectionCurvey(ax, listOfLabels,listOfLabels[label1Index], listOfLabels[label2Index], connectionMatrix[label1Index][ label2Index],"black")
    __displayCircosGraph(displayToScreen, fileName)

def __createCircosGraph(title, listOfNames):
    """
       #Creates the graph and adds labels.  Does not add any connections
    """
    #preprocessing
    tickPositions = [i*2*np.pi/len(listOfNames) for i in range(0,len(listOfNames))]

    #Sets up the polar plot
    ax = plt.subplot(111, polar=True)
    ax.set_rmax(1.0)
    #ax.set_rgrids([2,7,30,150], angle=0)#effectively removes the distance ticks if less than
    ax.grid(True)
    ax.set_title(title, va='bottom')

    #setup tick names and positions
    plt.xticks(tickPositions)
    ax.set_xticklabels(listOfNames)
    return ax

def __addCircosGraphConnectionCurvey(ax, listOfNames ,label1, label2, weight, color):
    """
       #Doesn't quite work perfectly for angles >90 degrees
       #Adds curvey lines instead of a straight one
    """
    if weight == 0:
        return
    #calculates all the positions of the labels
    tickPositions = [i*2*np.pi/len(listOfNames) for i in range(0,len(listOfNames))]

    position1 = listOfNames.index(label1)
    position2 = listOfNames.index(label2)
    angle = 2*np.pi/len(listOfNames)
    #Theta is the angle of the label, r is the outside of the circle.
    #Since we are dealing with a unit circle for ease of use, r is 1
    basePoints=[[1,position1*angle+angle*.1],[(1-(abs(position1*angle-position2*angle+weight*.1)/(np.pi+weight*.5)))/2,(position1*angle+position2*angle)/2],[1,position2*angle-angle*.1]]
    basePoints2=[[1,position2*angle+angle*.1],[(1-(abs(position1*angle-position2*angle-weight*.1)/(np.pi+weight*.5)))/2,(position1*angle+position2*angle)/2],[1,position1*angle-angle*.1]]
    startList = []
    startList2 = []
    for r,p in basePoints:
      x,y = __pol2cart(r,p)
      startList.append([x,y])
    for r,p in basePoints2:
      x,y = __pol2cart(r,p)
      startList2.append([x,y])

    middleList = __bezier_curve(startList, nTimes=10)
    middleList2 = __bezier_curve(startList2, nTimes=10)

    finalR = []
    finalTheta = []
    for x,y in zip(middleList[0],middleList[1]):
      r,p = __cart2pol(x,y)
      finalR.append(r)
      finalTheta.append(p)
    finalR2 = []
    finalTheta2 = []
    for x,y in zip(middleList2[0],middleList2[1]):
      r,p = __cart2pol(x,y)
      finalR2.append(r)
      finalTheta2.append(p)


    poly= mplPolygon(zip(finalTheta+finalTheta2,finalR+finalR2),True,alpha=.7)
    plt.gca().add_patch(poly)
#    ax.plot( finalTheta, finalR,  color=color, linewidth=weight)
    ax.ylim = 1
    ax.rlim = 1
    ax.xlim = 1

#Displays the graph after adding the points
def __displayCircosGraph(displayToScreen=False, writeToFile = None):
    """
    displays the created graph.  Can draw it to the screen, or save it to a file.
    """
    fig = plt.gcf()
    if writeToFile != None:
        fig.savefig(writeToFile)
    if displayToScreen == True:
        fig.show()



def __cart2pol(x, y):
    """
    converts cartesian to polar points
    """
    rho = np.sqrt(x**2 + y**2)
    phi = np.arctan2(y, x)
    return(rho, phi)

def __pol2cart(rho, phi):
    """
    coverts polar to cartesean points
    """
    x = rho * np.cos(phi)
    y = rho * np.sin(phi)
    return(x, y)


def __bezier_curve(points, nTimes=10):
    """
       Given a set of control points, return the
       bezier curve defined by the control points.

       points should be a list of lists, or list of tuples
       such as [ [1,1],
                 [2,3],
                 [4,5], ..[Xn, Yn] ]
        nTimes is the number of time steps, defaults to 1000

        See http://processingjs.nihongoresources.com/bezierinfo/
    """

    nPoints = len(points)
    xPoints = np.array([p[0] for p in points])
    yPoints = np.array([p[1] for p in points])

    t = np.linspace(0.0, 1.0, nTimes)

    polynomial_array = np.array([ __bernstein_poly(i, nPoints-1, t) for i in range(0, nPoints)   ])

    xvals = np.dot(xPoints, polynomial_array)
    yvals = np.dot(yPoints, polynomial_array)

    return xvals, yvals


def __bernstein_poly(i, n, t):
    """
     The Bernstein polynomial of n, i as a function of t
    """
    return comb(n, i) * ( t**(n-i) ) * (1 - t)**i



#------------------------------------------ Normal Graphing ------------------------------------------------
def graph(graphInfo, datasetInfo, saveLocation):
  """
  creates a graph of some data, and saves it to the saveLocation.
  graphinfo is a GraphInfo object
  datasetInfo is a DatasetParameters object
  """
  if(graphInfo.graphType in  ["scatter", "line", "bar", "line_with_errb"]):
    __graphPlot(graphInfo, datasetInfo,saveLocation)
  else:
    print("Illegal graphType:"+graphInfo.graphType)


# ----Data Structures----
class GraphInfo:
  """
  Contains all the parameters that are part of the graph and only need to be applied once.
  """
  def __init__(self, graphType, title="",xRange=None, yRange=None, fractional=False, xLabel="", yLabel="",xTickPosition = None, xTickAlias=None, yTickPosition = None, yTickAlias=None, xLogScaling=False, yLogScaling=False):
    self.graphType= graphType
    self.xRange = xRange#
    self.yRange = yRange#
    self.fractional = fractional
    self.title = title#
    self.xLabel = xLabel#
    self.yLabel = yLabel#
    self.xTickPosition = xTickPosition#
    self.yTickPosition = yTickPosition#
    self.xTickAlias = xTickAlias#
    self.yTickAlias = yTickAlias#
    self.xLogScaling = xLogScaling#
    self.yLogScaling = yLogScaling#

class DatasetParameters:
    """
    Contains all the parameters that are part of each data line, needing to be applied multiplied times.
    """
    name = None
    xy = None
    errorBarsHigh = None
    errorBarsLow = None
    color=None
    errwidth=None
    def __init__(self, xy=[[],[]],errorBarsHigh=None,errorBarsLow=None, color=None,name ="", errwidth=None):
      self.xy= xy#
      self.errorBarsHigh = errorBarsHigh#
      self.errorBarsLow = errorBarsLow#
      self.color= color#
      self.name = name#
      self.errwidth =  errwidth

def __unify2DLists(xy):
    """
    Will convert multiple different ways to orient data into numpyArray([x1,x2,x3,x4],[y1,y2,y3,y4]]...)
    """
    if(isinstance(xy, dict)):
      xy=xy.items()
    xy = np.asarray(xy)
    if(xy.shape[0]>2 and xy.shape[1]==2):
      xy = xy.T
    return xy

def __graphPlot(graphInfo, datasetsInfo,saveLocation):
  """
  Actually creates the graph, and set all the various features that exist in DatasetParameters and GraphInfo.
  """
  plotlyAllDatasets = []
  for datasetInfo in datasetsInfo:
    x=__unify2DLists(datasetInfo.xy)[0]
    y=__unify2DLists(datasetInfo.xy)[1]
    name= datasetInfo.name
    color = datasetInfo.color
    yError=dict()

    if graphInfo.graphType == "line" or graphInfo.graphType == "line_with_errb":
      mode="lines+markers"
    elif graphInfo.graphType == "scatter":
      mode="markers"
    if (not datasetInfo.errorBarsHigh == None) and  (not datasetInfo.errorBarsLow == None):
      modifiedLower = []
      modifiedHigher =[]
      for med, low, high in zip(y, datasetInfo.errorBarsLow,  datasetInfo.errorBarsHigh):
        modifiedLower.append(med- low)
        modifiedHigher.append(high-med)
      yError['array']=modifiedHigher
      yError['arrayminus']=modifiedLower
    if graphInfo.graphType=="bar":
      plotlyDataset = go.Bar(x=x, y=y,name=name, error_y=yError,
        marker = dict(color=color)
      )
    else:
      plotlyDataset = go.Scatter(x=x, y=y,name=name, error_y=yError,mode=mode,
        marker = dict(color=color)
      )
    plotlyAllDatasets.append(plotlyDataset)

  xaxis = dict( )
  xaxis['title']=graphInfo.xLabel
  xaxis['ticklen']=graphInfo.xTickPosition
  if(graphInfo.xLogScaling):
    xaxis['type']='log'
  if graphInfo.xRange ==None:
    xaxis['autorange']=True
  else:
    xaxis['autorange']=False
    xaxis['range'] = graphInfo.xRange
    xaxis['zeroline']=True
  if graphInfo.xTickPosition != None:
    xaxis['tickmode']="array"
    xaxis['tickvals']=graphInfo.xTickPosition
    if graphInfo.xTickAlias!=None:
      xaxis['ticktext'] = graphInfo.xTickAlias
  yaxis = dict( )
  yaxis['title']=graphInfo.yLabel
  if(graphInfo.yLogScaling):
    yaxis['type']='log'
  if graphInfo.yRange ==None:
    yaxis['autorange']=True
  else:
    yaxis['autorange']=False
    yaxis['range'] = graphInfo.yRange
    yaxis['zeroline']=True
  if graphInfo.yTickPosition != None:
    yaxis['tickmode']="array"
    yaxis['tickvals']=graphInfo.yTickPosition
    if graphInfo.yTickAlias!=None:
      yaxis['ticktext'] = graphInfo.yTickAlias

  layout = go.Layout(title='A Simple Plot', width=800, height=640,xaxis=xaxis, yaxis=yaxis)
  fig = go.Figure(data=plotlyAllDatasets, layout=layout)
  plot(fig,filename=saveLocation, auto_open=False)


if __name__ == "__main__":
    pass
