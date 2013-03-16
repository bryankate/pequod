import os, re

class GNUPlot:
    def __init__(self, datafile, prefix, xlabel, ylabel, ycolumnName1, ycolumnName2 = None):
        if datafile == None:
            self.f = None
            return
        xlabel = GNUPlot.escape(xlabel)
        ylabel = GNUPlot.escape(ylabel)
        self.datafile = os.path.basename(datafile)
        basename = prefix + "_" + os.path.basename(datafile).split('.')[0]
        self.f = open(os.path.join(os.path.dirname(datafile), basename + ".gnuplot"), "w")
        header = '''set output "%s.eps"
set yrange [0:]
set terminal postscript 'Times-Roman' eps dl 2 enhanced
set pointsize 1
#set size .7,.5
set zeroaxis
set xlabel "%s" font 'Times-Roman, 18pt'
set ylabel "%s" font 'Times-Roman, 18pt'
set key right top
set pointsize 1
''' % (basename, xlabel, ylabel)
        self.f.write(header)
        self.f.write("plot ")
        self.nline = 0
        self.linecolor = 1
        self.lastTitle = None
        self.ix = 1
        self.iy = None
        self.ycolumnName1 = ycolumnName1
        self.ycolumnName2 = ycolumnName2

    @staticmethod
    def columnIndex(names, n):
        for i in range(len(names)):
            if names[i].endswith(n): break
        if i == len(names) - 1:
            raise Exception('bad names')
        return i + 1

    def setColumnNames(self, names):
        if self.f == None: return
        if self.iy: return
        iy1 = GNUPlot.columnIndex(names, self.ycolumnName1)
        if not self.ycolumnName2:
            self.iy = str(iy1)
            return
        iy2 = GNUPlot.columnIndex(names, self.ycolumnName2)
        self.iy = "($%d+$%d)" % (iy1, iy2)

    def addLine(self, groupNumber, title):
        if self.f == None:
            return
        title = GNUPlot.escape(title)
        if self.nline > 0:
            self.f.write(',')
            if self.lastTitle != title:
                self.linecolor += 1
        color = ['red', 'green', 'blue']
        self.f.write(" \\\n    \"%s\" every :::%d::%d using %d:%s title \"%s\" with lp lt 1 lw 2 pt %d lc rgb \'%s\'" %
                     (self.datafile, groupNumber, groupNumber, self.ix, self.iy, title,
                      self.nline, color[self.linecolor % 3]))
        self.nline += 1
        self.lastTitle = title

    def __del__(self):
        if self.f == None:
            return
        self.f.write("\n")
        self.f.close()

    @staticmethod
    def escape(gnuString):
        return re.sub("_", "-", gnuString)

if __name__ == "__main__":
    g = GNUPlot("test.data", "x", "y")
    g.addLine(0, 1, 2, "hello")
    g.addLine(0, 1, 2, "hello")
