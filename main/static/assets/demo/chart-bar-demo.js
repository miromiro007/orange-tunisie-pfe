// Set new default font family and font color to mimic Bootstrap's default styling
Chart.defaults.global.defaultFontFamily = '-apple-system,system-ui,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif';
Chart.defaults.global.defaultFontColor = '#292b2c';
'use strict';
// Bar Chart Example
var ctx = document.getElementById("myBarChart");

var link_status_data = document.getElementById("link_status_data").value;
var link_status_labels = document.getElementById("link_status_labels").value;

console.log(typeof link_status_labels );
link_status_labels = link_status_labels.substring(1, link_status_labels.length - 1);
link_status_labels = link_status_labels.split(",");
link_status_data = JSON.parse(link_status_data)

var myLineChart = new Chart(ctx, {
  type: 'bar',
  data: {
    labels: link_status_labels,
    datasets: [{
      label: "Nombre de Liens",
      backgroundColor: "rgba(2,117,216,1)",
      borderColor: "rgba(2,117,216,1)",
      data: link_status_data,
    }],
  },
  options: {
    scales: {
      xAxes: [{
        time: {
          unit: 'month'
        },
        gridLines: {
          display: false
        },
        ticks: {
          maxTicksLimit: 6
        }
      }],
      yAxes: [{
        ticks: {
          min: 0,
          max: 5000,
          maxTicksLimit: 5
        },
        gridLines: {
          display: true
        }
      }],
    },
    legend: {
      display: false
    }
  }
});
