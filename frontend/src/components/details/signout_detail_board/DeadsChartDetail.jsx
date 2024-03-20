import React from 'react';
import { useNavigate} from 'react-router-dom';
import { Bar } from 'react-chartjs-2';
import Chart from 'chart.js/auto';
import AnnotationPlugin from 'chartjs-plugin-annotation';
import ChartDataLabels from 'chartjs-plugin-datalabels';
import '../../charts/arrived_chart.css';

Chart.register(AnnotationPlugin);
Chart.register(ChartDataLabels);


Chart.defaults.font.size = 12;
Chart.defaults.color = '#090b1f';  

// Depts map
const chartMap = {
    'oar1_d': 'ОРИТ №1',
    'dp_d': 'Приемное отделение',
    'oar2_d': 'ОРИТ №2',
    'oar3_d': 'ОРИТ №3',
    'cardio_d': 'Кардиологическое отделение',
    'surgery_d': 'Хирургическое отделение',
    'therapy_d': 'Терапевтическое отделение',

}

const DeadsChartDetail = ({ data }) => {

    const navigate = useNavigate();


    const deptsOut = Object.fromEntries(
        Object.entries(data)
          .filter(([key, value]) => key.endsWith('_d'))
      );

    console.log(deptsOut)
    const endDepts = Object.keys(deptsOut)
    const ruDepts = endDepts.map(index => chartMap[index])
    const nums = Object.values(deptsOut) 


    const plan = 60

    const arrived_data = {
        labels: ruDepts,
        datasets: [
            {
              label: 'total',
              data: nums,
              backgroundColor: ['#2d8587'],
              borderColor: '#090b1f',
              borderWidth: 1,
            },
        ],
    };

    const chartOptions = {
        barThickness: 'flex',
        barPercentage: 0.9, 
        categoryPercentage: 0.8,
        scales: {
            x: {
                // stacked: true,
                grid: { 
                  drawOnChartArea: false,
                  drawTicks: false
                },
                ticks: {
                    // display: false,
                    beginAtZero: true,
                    color: '#090b1f',   
                    font: {
                        // size: 14,
                        weight: 'bold' 
                },},
            },
            y: {
                // stacked: true,
                // min: 0, 
                // max: 100, 
                grid: {
                  drawOnChartArea: true,
                  drawTicks: false
                  },
             
                ticks: {
                    font: {
                      weight: 'bold'
                    },
                },
            },
        },

        plugins: { 
            datalabels: {
                display: true,
                labels: {
                    title: {
                        color: 'black',
                        font: {
                          size: 13,
                        //   weight: 'bold',
                          },
                        anchor: 'end',
                        align: 'end',
                    },
                    // Values inside the bars
                    // value: {
                    //     formatter: title => {
                    //         const percernts = ((title / plan  * 100) - 100).toFixed(1);
                    //         return '\t' + percernts+'%';
                    //     },
                    //     color: 'blue',
                    //     font: {
                    //       size: 10,
                    //       weight: 'bold',
                    //       },
                    // },
                },
            },
            legend: {
                display: false,
            },
            title: {
                display: true,
                text: 'Выписанные по отделениям',
                color: '#090b1f',
                font: {
                    size: 13,
            },                
            padding: {
            //   top: 20, // Adjust the top padding as needed
              bottom: 30,
            }
            },
    
        },

        /* This code makes bars on chart clickable  */

        // onClick: function () {
        //         var link = '/deads'; 
        //         navigate(link); // Changes the current page's URL
        // }
    };

    // Chart component
    return (
        <div className='arrived_chart'>
          <Bar data={arrived_data} options={chartOptions} />
        </div>
    );
};

export default DeadsChartDetail;