import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt

#######   API and supporting methods   #############


#Will convert multiple different ways to orient data into numpyArray([x1,x2,x3,x4],[y1,y2,y3,y4]]...)
def unify2DLists(xy):
    if(isinstance(xy, dict)):
      xy=xy.items()
    xy = np.asarray(xy)
    if(xy.shape[0]>2 and xy.shape[1]==2):
      xy = xy.T
    return xy

#TODO improve log function capabilities, including proper zoom
def graphPlot(graphInfo, datasetsInfo,saveLocation):
  #Values that only need to be done once
  sns.set()
  #f, ax = sns.plt.subplots(figsize=(7, 7))

  plt.title(graphInfo.title)
  if(graphInfo.xRange!=None):
    plt.xlim(graphInfo.xRange)#rework this a little bit
  if(graphInfo.yRange!=None):
    plt.ylim(graphInfo.yRange)#rework this a little bit
  plt.ylabel(graphInfo.yLabel)
  plt.xlabel(graphInfo.xLabel)
  if(graphInfo.xTickPosition!=None and graphInfo.xTickAlias!=None): 
    plt.xticks(graphInfo.xTickPosition, graphInfo.xTickAlias)#, rotation='vertical')
  if(graphInfo.yTickPosition!=None and graphInfo.yTickAlias!=None): 
    plt.yticks(graphInfo.yTickPosition, graphInfo.yTickAlias)#, rotation='vertical')
  #sns.plt.setp(labels, rotation=45)
  #Values that need to be done for each dataset/line
  for datasetInfo in datasetsInfo:
    arrayX=unify2DLists(datasetInfo.xy)[0]
    arrayY=unify2DLists(datasetInfo.xy)[1]

    if(graphInfo.graphType=="scatter"):
      #Scatterplot    
      print(graphInfo.graphType)
      plot = sns.regplot(x=arrayX,y=arrayY,fit_reg=False,color=datasetInfo.color, label=datasetInfo.name)
    elif(graphInfo.graphType=="line"):
      #Lineplot
        #plot = sns.pointplot(x=arrayX,y=arrayY,color=datasetInfo.color, linestyle='-')
        plot = plt.plot(arrayX, arrayY,label=datasetInfo.name)
    elif(graphInfo.graphType=="bar"):
      #Lineplot
      plot = sns.barplot(x=arrayX,y=arrayY,color=datasetInfo.color,label=datasetInfo.name)

  frameon=True
  plt.legend()
  if(graphInfo.xTickPosition!=None and graphInfo.xTickAlias!=None): 
    plt.xticks(graphInfo.xTickPosition, graphInfo.xTickAlias)#, rotation='vertical')
  if(graphInfo.yTickPosition!=None and graphInfo.yTickAlias!=None): 
    plt.yticks(graphInfo.yTickPosition, graphInfo.yTickAlias)#, rotation='vertical')
    if(graphInfo.xLogScaling==True):
      plt.xscale("log")
    if(graphInfo.yLogScaling==True):
      plt.yscale("log")
  #sns.plt.show()
  plt.savefig(saveLocation, dpi=1200)

# TODO: a more modern use   
# x = sns.pointplot(x=dx.index,
#                 y="value",
#                 data=dx, hue="variable",
#                 scale=0.5, dodge=True,
#                 capsize=.2)

# h,l = ax.get_legend_handles_labels()

# l = ["child", "teen"]

# ax.legend(h, l)
# plt.show()
