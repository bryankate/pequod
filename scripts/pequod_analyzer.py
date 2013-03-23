import os, json, sys, csv, math, struct
from lib.gnuplot import GNUPlot
from sets import Set

class ResultAnalyzer:
    def __init__(self, xlabel, ename):
        self.exp = {}
        self.xlabel = xlabel
        self.ename = ename
    
    def add(self, plotgroup, resultDir, plotkey):
        ntrail = 0
        r = 0
        aj = {}
        for f in os.listdir(resultDir):
            if not f.endswith(".json"):
                continue
            fpath = os.path.join(resultDir, f)
            j = json.load(open(fpath))
            if not aj:
                for k,v in j.items():
                    if isinstance(v, (int, long, float)):
                        aj[k] = v
            else:
                for k in aj.keys():
                    aj[k] += j[k]
            ntrail += 1
        # average
        for k,v in aj.items():
            aj[k] = v / ntrail
        if not self.exp.has_key(plotgroup):
            self.exp[plotgroup] = []
        self.exp[plotgroup].append([('#_X_axis', plotkey)] + aj.items())

    def getGNUData(self, fname, header = None):
        if fname == None:
            f = sys.stderr
        else:
            f = open(fname, "w")

        graphs = []
        if self.ename.startswith("rwmicro") or self.ename.startswith("ehash") or self.ename.startswith("client_push") or self.ename.startswith('remote_client_push'):
            graphs.append(GNUPlot(fname, "runtime", self.xlabel, "Runtime(second)", 
                                  "real_time", xcolumnName = "actual_prefresh"))
        elif self.ename == "policy":
            graphs.append(GNUPlot(fname, "runtime", self.xlabel, "Runtime(second)", 
                                  "real_time", xcolumnName = "inactive"))
        elif self.ename.startswith == "real_twitter":
            graphs.append(GNUPlot(fname, "runtime", self.xlabel, "Runtime (s)", 
                                  "real_time", xcolumnName = "real_time"))
       
        print >> f, "#", header
        plotgroups = sorted(self.exp.keys())
        for groupNumber, plotgroup in enumerate(plotgroups):
            points = self.exp[plotgroup]
            nameset = set()
            for p in points:
                for x in p:
                    nameset.add(x[0])
            names = [("%30s" % x) for x in sorted(nameset)]            
            for x in names:
                if len(x.strip().rstrip()) >= 30:
                    raise Exception('%s too long' % x)
            print >> f, "#", "".join([("%20d" % (x + 1)) for x in range(len(names))])
            print >> f, "#", "".join(names)
            # sort by plotkey (only if a number)
            if len(points) and isinstance(points[0][0][1], (int, long, float)):
                points = sorted(points)
            for p in points:
                data = [("%20.4f" % 0)] * len(names)
                for idx in range(len(names)):
                    pnames = [x[0] for x in p]
                    pnames = [("%30s" % x) for x in pnames]                
                    for x in pnames:
                        if len(x.strip().rstrip()) >= 30:
                            raise Exception('%s too long' % x)
                    n = names[idx]
                    if n in pnames:
                        x = p[pnames.index(n)][1]
                        data[idx] = (("%20.4f" % x) if isinstance(x, (int, long, float)) else x)
                print >> f, " ", "".join(data)
            print >> f
            for g in graphs:
                g.setColumnNames(names)
                g.addLine(groupNumber, plotgroup)
        if f != sys.stderr:
            f.close()

    def getCSVHorizon(self, fname, header):
        if fname == None:
            f = sys.stderr
        else:
            existed = os.path.exists(fname)
            f = open(fname, "a")
        cf = csv.writer(f, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        cf.writerow(["#" + header])
        plotgroups = sorted(self.exp.keys())
        if not existed:
            cf.writerow(["# note that the bandwidth is not real? Traffic between servers are double counted?"])
            names = [x[0] for x in self.exp[plotgroups[0]][0]]
            index = range(1, len(names) + 1)
            cf.writerow([("%d" % x) for x in index])
            cf.writerow(names)

        for plotgroup in plotgroups:
            points = self.exp[plotgroup]
            # sort by plotkey
            points = sorted(points)
            for p in points:
                cf.writerow([(("%.2f" % x[1]) if isinstance(x[1], (int, long, float)) else x[1]) for x in p])
        cf.writerow([])
        if f != sys.stderr:
            f.close()

if __name__ == "__main__":
    def reanalyze(expdir):
        for ename in os.listdir(expdir):
            epath = os.path.join(expdir, ename)
            if not os.path.isdir(epath):
                continue
            print 'Experiment', ename
            a = ResultAnalyzer('refresh ratio(%)', ename)
            for load in os.listdir(epath):
                loadpath = os.path.join(epath, load)
                if not os.path.isdir(loadpath):
                    continue
                x = load.split('_')
                print '\tWorkload', x
                a.add('_'.join(x[0:-1]), loadpath, float(x[-1]))
            a.getGNUData(None, "")
            a.getGNUData(os.path.join(epath, ename + '.data'))
            a.getCSVHorizon("./results/notebook.csv", expdir)

    if len(sys.argv) == 1:
        reanalyze("./results/2013_03_20-13_32_36")
    else:
        reanalyze(sys.argv[1])
