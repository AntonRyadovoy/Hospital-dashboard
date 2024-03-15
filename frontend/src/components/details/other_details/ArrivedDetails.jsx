import React, { useState, useEffect, useCallback } from 'react';
import { useLocation } from 'react-router-dom';
import GetData from '../../GetData';
import ArrivedDetailPie from './ArrivedPieChart';
import SignoutDetailPie from './SignoutPieChart';
import NoConnection from '../../no_data/NoConnection';


const ArrivedDetails = () => {
    const location = useLocation();

    const srcUrl = window.location.href;
    const date = srcUrl.split('=').pop();
    const urlParams = srcUrl.split('/').pop();
    const dataType = srcUrl.split('=')[1].split('&')[0];  

    let data = sessionStorage.getItem(`${dataType}_${date}`);


    if (data) {
      data = JSON.parse(data);
    } else {
      const fetchedData = GetData(`http://localhost:8000/api/v1/${urlParams}`);
      if (fetchedData) {
        sessionStorage.setItem(`${dataType}_${date}`, JSON.stringify(fetchedData));
        data = fetchedData;
      }
    }
    

    let pie;

    if (data) {

      const keys = Object.keys(data['data']['0'])
      const values = Object.values(data['data']['0'])

        if (keys[0] === 'deads') {
          pie = <SignoutDetailPie labels={keys} data={values} />
        } else {
          pie = <ArrivedDetailPie labels={keys} data={values} />
        }

      

      return (pie);
    } else {
      return (
        <NoConnection />
      );
    }
};

export default ArrivedDetails;