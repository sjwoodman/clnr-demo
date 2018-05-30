<%-- 
    Document   : index
    Created on : 25-May-2018, 12:11:02
    Author     : hhiden
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html lang="en">
    <head>
        <meta charset="UTF-8">
        <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/patternfly/3.24.0/css/patternfly.min.css">
        <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/patternfly/3.24.0/css/patternfly-additions.min.css">
        <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/c3/0.6.1/c3.min.css">
    </head>
    <body>
        <div class="container">
            <h1>Current demand running total:</h1>
            <span id="result"></span>

            <div id="donut-chart-5"></div>
        </div>

        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.2.1/jquery.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/3.3.7/js/bootstrap.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/patternfly/3.24.0/js/patternfly.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/4.13.0/d3.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/c3/0.6.1/c3.min.js"></script>


        <script>
            var demandHistory = {};

            var port = "";
            if (window.location.host.search(".rhcloud.com") > 0) {
                port = ":8000";
            }
            var url = 'ws://' + window.location.host + port + window.location.pathname + 'ws';
            var ws = new WebSocket(url);
            ws.onconnect = function (e) {
                console.log("connected");
            }
            ws.onerror = function (error) {
                console.log('WebSocket Error ' + error);
            };
            ws.onclose = function (event) {
                console.log("Remote host closed or refused WebSocket connection");
                console.log(event);
            };
            ws.onmessage = function (message) {
                document.getElementById("result").innerHTML = message.data;
                updateGraph(JSON.parse(message.data));
            };

            function updateGraph(demandLevel){
                demandHistory[demandLevel.timedate] = demandLevel.demand;
                
                var xData = Object.keys(demandHistory);
                var yData = new Array();
                var ySeries = new Array();
                ySeries.push("kWh");
                for(var i=0;i<xData.length;i++){
                    ySeries.push(demandHistory[keys[i]]);
                }
                yData.push(ySeries);
                
            }
            
        </script>        
    </body>
</html>