var ngraphs = 0;
var graphs;
var lines;
var raw;
var colors = {client: "#444444", backend: "#0060ad", cache: "#dd181f"};
var symbols = {client: "circle", backend: "square", cache: "triangle"};

function clearGraphs() {
  graphs = document.getElementById('graphs');
  for (var i = 0; i < ngraphs; ++i)
    graphs.removeChild(document.getElementById('graph_' + i.toString()));
  ngraphs = 0;
  lines = {};
}

function redraw(rawjson) {
  clearGraphs();
  raw = rawjson;
  
  if (rawjson.hasOwnProperty('log'))
     processLog(rawjson['log'], 'client_0', colors['client'], symbols['client']);
  else if (rawjson.hasOwnProperty('client_logs'))
    for (var l = 0; l < rawjson['client_logs'].length; ++l)
      processLog(rawjson['client_logs'][l], 'client_' + l, colors['client'], symbols['client']);
  
  if (rawjson.hasOwnProperty('server_logs') && rawjson['server_logs']) {
    var b = 0;
    var c = 0;
    for (var l = 0; l < rawjson['server_logs'].length; ++l)
      if (rawjson['server_logs'][l]['backend'])
        processLog(rawjson['server_logs'][l].data, 'backend_' + b++, colors['backend'], symbols['backend']);
      else
        processLog(rawjson['server_logs'][l].data, 'cache_' + c++, colors['cache'], symbols['cache']);
  }

  for (g in lines) {
    var plotid = createGraph(g);
    var alldata = [];
    var i = 0;
    
    for (l in lines[g])
      alldata.push({data: lines[g][l].data, 
                    label : l, 
                    color : lines[g][l].color, 
                    points: { symbol: lines[g][l].symbol }});
    
    var options = { series: { lines: { show: true },
                              points: { show: true }},
                    grid: { hoverable: true, clickable: true },
                    xaxis: {axisLabel: "Time (s)", axisLabelPadding: 10},
                    yaxis: {axisLabel: g, axisLabelPadding: 10, min: 0},
                    legend: {show: false, position: "nw"}}
    
    if (g.indexOf("_pct") !== -1)
      options.yaxis['max'] = 105;
                    
    plotGraph(alldata, plotid, options);
  }
}

function processLog(logjson, pname, clr, sym) {
  for (key in logjson) {
    if (key.indexOf("time_us") !== -1)
      continue;
    if (!lines.hasOwnProperty(key))
      lines[key] = {};
    
    line = {data: [], color: clr, symbol: sym};
    for (var i = 0; i < logjson[key].length; ++i)
      line.data.push([logjson[key][i][0] / 1000000.0, logjson[key][i][1]]);
    
    lines[key][pname] = line;
  }
}

function createGraph(gname) {
  graphs = document.getElementById('graphs');
  
  var gtitle = document.createElement('div');
  gtitle.innerHTML = '<h3>' + gname + '</h3>';
  
  var gplot =  document.createElement('div');
  var plotid = 'plot_' + ngraphs;
  gplot.style.width = '900px';
  gplot.style.height = '350px';
  gplot.id = plotid;
  
  var graph = document.createElement('div');
  graph.id = 'graph_' + ngraphs;
  
  graph.appendChild(gtitle);
  graph.appendChild(gplot);
  graphs.appendChild(graph);
  
  ++ngraphs;
  return plotid;
}

function showTooltip(tid, x, y, contents) {
    $('<div id="'+ tid + '">' + contents + '</div>').css( {
        position: 'absolute',
        display: 'none',
        top: y + 5,
        left: x + 5,
        border: '1px solid #000000',
        padding: '5px',
        color: '#ffffff',
        'font-size': '12px',
        'background-color': '#333333',
        opacity: 0.80
    }).appendTo("body").fadeIn(200);
}

function plotGraph(lines, id, options) {
    var tid = id + "_tooltip";
    var plotgid = "#" + id;
    var plottid = "#" + tid;
    $.plot($(plotgid), lines, options);

    var previousPoint = null;
  
    $(plotgid).bind("plothover", function (event, pos, item) {
        if (!item) {
            $(plottid).remove();
            previousPoint = null;            
            return;             
        }
        if (previousPoint != item.dataIndex) {
            previousPoint = item.dataIndex;
            $(plottid).remove();
            var ttip = item.series.label + ", (" + 
                       item.datapoint[0].toFixed(3) + ", " + 
                       item.datapoint[1].toFixed(3) + ")";

            showTooltip(tid, item.pageX, item.pageY, ttip);
        }
    });
    
    $(plotgid).bind("plotclick", function (event, pos, item) {
        $(plottid).remove();
    });
}
