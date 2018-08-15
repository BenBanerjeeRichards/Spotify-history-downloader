DATA = {"top_tracks": [], "count": 0};

window.onload = function() {
    var app = new Vue({
        el: '#top-tracks',
        data: DATA
    });

    var n_plays = new Vue({
        el: "#num-plays",
        data: DATA
    });

    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            DATA = JSON.parse(this.responseText);
            console.log(DATA);
            Vue.set(app.data, "top_tracks", DATA["top_tracks"])
        }
    };
    xhttp.open("GET", "http://localhodst:8080/spotify", true);
    xhttp.send();

    $.getJSON()

    var ctx = document.getElementById("timeGraph").getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ["midnight", "1am", "2am", "3am", "4am", "5am", "6am", "7am", "8am", "9am", "10am", "11am", "12am", "1pm", "2pm", "3pm", "4pm", "5pm", "6pm", "7pm", "8pm", "9pm", "10pm", "11pm"],
            datasets: [{
                label: '# of plays',
                data: DATA["time_dist"],
                backgroundColor: [
                    'rgba(255, 99, 132, 0.2)',
                    'rgba(54, 162, 235, 0.2)',
                    'rgba(255, 206, 86, 0.2)',
                    'rgba(75, 192, 192, 0.2)',
                    'rgba(153, 102, 255, 0.2)',
                    'rgba(255, 159, 64, 0.2)'
                ],
                borderColor: [
                    'rgba(255,99,132,1)',
                    'rgba(54, 162, 235, 1)',
                    'rgba(255, 206, 86, 1)',
                    'rgba(75, 192, 192, 1)',
                    'rgba(153, 102, 255, 1)',
                    'rgba(255, 159, 64, 1)'
                ],
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            legend: false,

            scales: {
                yAxes: [{
                    gridLines: {
                        color: "rgba(0, 0, 0, 0)"
                    },

                    ticks: {
                        beginAtZero:true
                    }
                }],
                xAxes: [{
                    gridLines: {
                        color: "rgba(0, 0, 0, 0)"
                    }
                }]
            }
        }
    });
};