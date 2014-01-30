import os
import sys
import json
import csv
import numpy as np
import pandas as pd

def extract_data(dir, data):
    if os.path.exists(os.path.join(dir, "aggregated.json")):
        pfile = open(os.path.join(dir, "aggregated.json"))
    else:
        pfile = open(os.path.join(dir, "output_app_0.json"))
    
    jdata = json.load(pfile)
    pfile.close()
    
    if data['from'] == "client":
        return str(jdata[data['attr']])
    elif data["from"] == "server":
        val = None
        agg = data['agg'] if 'agg' in data else "avg"
        for s in jdata['server_stats']:
            v = s[data['attr']]
            
            if not val:
                val = v
            elif agg == "min" and v < val:
                val = v
            elif agg == "max" and v > val:
                val = v
            else:
                val = val + v

        if val and agg == "avg":
            val = val / len(jdata['server_stats'])
        return str(val if val else 0)
    
def write_common(pfile, name, xlabel, ylabel):
    pfile.write("set term postscript eps size 5,3 enhanced 20\n" + \
                "set output '" + name + ".eps'\n\n" + \
                "set xlabel '" + xlabel + "'\n" + \
                "set ylabel '" + ylabel + "'\n" + \
                "set yrange [0:*]\n\n")

def write_common_line(pfile, name, xlabel, ylabel):
    write_common(pfile, name, xlabel, ylabel)
    pfile.write("set style line 1 lc rgb '#000000' lt 1 lw 2 pt 5   # black\n" + \
                "set style line 2 lc rgb '#0060ad' lt 1 lw 2 pt 6   # blue\n" + \
                "set style line 3 lc rgb '#dd181f' lt 1 lw 2 pt 7   # red\n" + \
                "set style line 4 lc rgb '#5e9c36' lt 1 lw 2 pt 8   # green\n\n")
    
def write_common_bar(pfile, name, xlabel, ylabel):
    write_common(pfile, name, xlabel, ylabel)
    pfile.write("set style line 1 lc rgb '#696969' lt 1 lw 2 pt 5\n" + \
                "set style line 2 lc rgb '#a9a9a9' lt 1 lw 2 pt 5\n" + \
                "set style line 3 lc rgb '#808080' lt 1 lw 2 pt 5\n" + \
                "set style line 4 lc rgb '#d3d3d3' lt 1 lw 2 pt 5\n\n" + \
                "set style data histogram\n" + \
                "set style fill solid noborder\n" + \
                "set auto x\n")

def make_data(expname, resdir, params):
    
    datapath = os.path.join(resdir, expname + ".dat")
    datafile = open(datapath, "w")
    datafile.write("#exp");
    
    for d in params['data']:
        datafile.write('\t' + d['attr'])
    datafile.write("\n" + expname + "\t")
    
    for d in params['data']:
        datafile.write('\t' + extract_data(os.path.join(resdir), d))
    
    datafile.write("\n")
    datafile.close()

def make_lineplot(expname, resdir, params):
    dcount = 0
    
    for d in params['data']:
        plotname = expname + "_" + d['attr']
        datapath = os.path.join(resdir, plotname + ".dat")
        datafile = open(datapath, "w")
        
        datafile.write("#x")
        for l in params['lines']:
            datafile.write("\t" + d['attr'] + '_' + l)
        datafile.write("\n")
        
        # row for each point
        for p in params['points']:
            datafile.write(str(p))
            
            for l in params['lines']:
                datafile.write('\t' + extract_data(os.path.join(resdir, l + "_" + str(p)), d))
            datafile.write("\n")
        datafile.close()

        plotfile = open(os.path.join(resdir, plotname + ".plt"), "w")
        ylabel = params['ylabel'][dcount] if isinstance(params['ylabel'], (list)) else params['ylabel']
        write_common_line(plotfile, plotname, params['xlabel'], ylabel)
        
        plotfile.write("plot ")
        for i in range(len(params['lines'])):
            plotfile.write("'" + plotname + ".dat' u 1:" + str(i+2) + \
                           " w l ls " + str(i+1) + " t '" + params['lines'][i] + "'")
            if i < len(params['lines']) - 1:
                plotfile.write(", \\\n")
        
        plotfile.close()
        dcount += 1

def make_barplot(expname, resdir, params):
    dcount = 0

    for d in params['data']:
        plotname = expname + "_" + d['attr']
        datapath = os.path.join(resdir, plotname + ".dat")
        datafile = open(datapath, "w")
        
        datafile.write("bar\t" + d['attr'] + "\n")
        
        # row for each bar
        for l in params['lines']:
            datafile.write(l + "\t" + extract_data(os.path.join(resdir, l), d) + "\n")
        datafile.close()

        plotfile = open(os.path.join(resdir, plotname + ".plt"), "w")
        xlabel = params['xlabel'] if 'xlabel' in params else ""
        ylabel = params['ylabel'][dcount] if isinstance(params['ylabel'], (list)) else params['ylabel']
        write_common_bar(plotfile, plotname, xlabel, ylabel)
        
        plotfile.write("set style histogram cluster gap 1\n" + \
                       "set boxwidth 0.9\n" + \
                       "unset key\n\n")
        
        plotfile.write("plot '" + plotname + ".dat' u 2:xtic(1) lt 1 fs solid 0.6 t col\n")
        plotfile.close()
        dcount += 1
    
def make_stackedbarplot(expname, resdir, params):  
    plotname = expname
    datapath = os.path.join(resdir, plotname + ".dat")
    datafile = open(datapath, "w")
    
    datafile.write("bar")
    for d in params['data']:
        datafile.write('\t' + d['attr'])
    datafile.write("\n")
    
    # row for each bar
    for l in params['lines']:
        datafile.write(l)
        for d in params['data']:
            datafile.write('\t' + extract_data(os.path.join(resdir, l), d))
        datafile.write("\n")
    datafile.close()

    plotfile = open(os.path.join(resdir, plotname + ".plt"), "w")
    xlabel = params['xlabel'] if 'xlabel' in params else ""
    write_common_bar(plotfile, plotname, xlabel, params['ylabel'])
    
    plotfile.write("set style histogram rowstacked\n" + \
                   "set boxwidth 0.5\n" + \
                   "set key top right reverse Left\n\n")
    
    plotfile.write("plot ")
    for i in range(len(params['data'])):
        plotfile.write("'" + plotname + ".dat' u " + str(i+2) + ":xtic(1) ls " + str(i+1) + " t col")
        if i < len(params['data']) - 1:
            plotfile.write(", ")
    plotfile.close()

def make_agg_data(resdir, expname, repeat):
    dfs = []
    for r in range(repeat):
        df = pd.io.parsers.read_csv(os.path.join(resdir, str(r), expname + ".dat"), 
                                    sep='\t', index_col=0)
        df = df.dropna(axis=1)
        dfs.append(df)
        
    alldf = pd.concat(dfs).groupby(level=0).agg([np.mean, np.std, np.min, np.max])
    
    with open(os.path.join(resdir, expname + ".dat"), "w") as fp:
        fp.write("#")
        alldf.to_csv(fp, sep='\t', header=["_".join(c) for c in alldf.columns])

def make_summary(expname, resdir, params, repeat):
    cols = [d['attr'] for d in params['data']]
    
    if params['type'] == 'line':
        dcount = 0
        for c in cols:
            plotname = expname + "_" + c
            make_agg_data(resdir, plotname, repeat)
            
            plotfile = open(os.path.join(resdir, plotname + ".plt"), "w")
            ylabel = params['ylabel'][dcount] if isinstance(params['ylabel'], (list)) else params['ylabel']
            write_common_line(plotfile, plotname, params['xlabel'], ylabel)
            
            plotfile.write("plot ")
            for i in range(len(params['lines'])):
                plotfile.write("'" + plotname + ".dat' u 1:" + str(i+2) + \
                               " w l ls " + str(i+1) + " t '" + params['lines'][i] + "', \\\n     " + \
                               "'" + plotname + ".dat' u 1:" + str(i+2) + ":" + str(i+4) + ":" + str(i+5) + \
                               " w errorbars ls " + str(i+1) + " notitle")
                if i < len(params['lines']) - 1:
                    plotfile.write(", \\\n")
            
            plotfile.close()
            dcount += 1
            
    elif params['type'] == 'bar':
        dcount = 0
        for c in cols:
            plotname = expname + "_" + c
            make_agg_data(resdir, plotname, repeat)
            
            plotfile = open(os.path.join(resdir, plotname + ".plt"), "w")
            xlabel = params['xlabel'] if 'xlabel' in params else ""
            ylabel = params['ylabel'][dcount] if isinstance(params['ylabel'], (list)) else params['ylabel']
            write_common_bar(plotfile, plotname, xlabel, ylabel)
            
            plotfile.write("set style histogram errorbars gap 1\n" + \
                           "set boxwidth 0.9\n" + \
                           "unset key\n\n")
            
            plotfile.write("plot '" + plotname + ".dat' u 2:4:5:xtic(1) w hist lt 1 fs solid 0.6 t col")
                    
            plotfile.close()
            dcount += 1
                
    elif params['type'] == 'data' or params['type'] == 'stackedbar':
        make_agg_data(resdir, expname, repeat)
       
def make_gnuplot(expname, resdir, params):
    if params['type'] == 'data':
        make_data(expname, resdir, params)
    elif params['type'] == 'line':
        make_lineplot(expname, resdir, params)
    elif params['type'] == 'bar':
        make_barplot(expname, resdir, params)
    elif params['type'] == "stackedbar":
        make_stackedbarplot(expname, resdir, params)

def make_gnuplots(expname, resdir, params, repeat):
    if (repeat == 1):
        make_gnuplot(expname, resdir, params)
    else:
        for r in range(repeat):
            rdir = os.path.join(resdir, str(r))
            make_gnuplot(expname, rdir, params)
        make_summary(expname, resdir, params, repeat)
        