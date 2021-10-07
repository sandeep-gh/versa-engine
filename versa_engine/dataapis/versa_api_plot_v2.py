#import dataapis.graphingAPI as papi ###plotting api #can't import seaborn due to python 3.10-cython-scipy issues
from dataapis.export import scan

def plot_X_vs_Ys(session=None, restype=None, stmt=None):
    plot_title= restype['plot']['title']
    xlabel=restype['plot']['xlabel']
    ylabel=restype['plot']['xlabel']
    pcontext = papi.GraphInfo(graphType="line",title=plot_title,xLabel=xlabel,yLabel=ylabel)
    plot_data=[]
    xValues = []
    aquery_res = scan(session, stmt)[0]
    if restype['xtype'] == 'interval':
        x_lb_key= restype['x_lb']
        x_ub_key= restype['x_ub']
        xValues = [(x[0] + x[1])/2 for x in zip(aquery_res._asdict()[x_lb_key], aquery_res._asdict()[x_ub_key])]
    if restype['xtype'] == 'enum':
        xValues = aquery_res._asdict()[restype['x_attr']]
    for yattr,ylabel in zip(restype['yattrs'], restype['ylabels']):
        plot_data.append(papi.DatasetParameters(xy=[xValues, aquery_res._asdict()[yattr]]))

    papi.graph(pcontext, plot_data, plot_title+'.pdf')
    return


def plot_XY(session=None, plot_info=None, stmt=None):
    plot_title= plot_info['plot']['title']
    xlabel=plot_info['plot']['xlabel']
    ylabel=plot_info['plot']['ylabel']

    plot_data=[]
    xValues = []
    aquery_res = scan(session, stmt)[0]
    xValues = aquery_res._asdict()[plot_info['x_attr']]
    y_attr = plot_info['y_attr']
    plot_data.append(papi.DatasetParameters(xy=[xValues, aquery_res._asdict()[y_attr]]))
    ylog=False
    if ylog in plot_info['plot']:
        if  plot_info['plot']['ylog'] == True:
            ylog = True

    graphType = plot_info['graphType']
    plot_context = papi.GraphInfo(graphType="scatter",title=plot_title,xLabel=xlabel,yLabel=ylabel, yLogScaling=ylog)
    papi.graph(plot_context, plot_data, plot_title+'.pdf')
    return


def plot_XY_by_Zs(session=None, restype=None, stmt=None):
    plot_title= restype['plot']['title']
    xlabel=restype['plot']['xlabel']
    ylabel=restype['plot']['ylabel']
    pcontext = papi.GraphInfo(graphType="line",title=plot_title,xLabel=xlabel,yLabel=ylabel)
    plot_data=[]
    x_attr = restype['x_attr']
    y_attr = restype['y_attr']
    z_attr = restype['z_attr']
    
    for res in scan(session, stmt):
        cat = res._asdict()[z_attr]
        x_vals = res._asdict()[x_attr]
        y_vals = res._asdict()[y_attr]
        plot_data.append(papi.DatasetParameters(xy=[x_vals, y_vals]))
    papi.graph(pcontext, plot_data, plot_title+'.pdf', set_ylog=True)
    return

