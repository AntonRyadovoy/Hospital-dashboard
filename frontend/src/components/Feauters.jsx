import { format, subHours } from 'date-fns';
import { formatDate } from './dates/DatesFormat';

export const currentDatetime = new Date().toLocaleDateString('ru-RU');


export function DateFormatting(date) {
  // const dateString = "2024-01-10T00:40:15+03:00";
  const dateTimeObject = new Date(date);
  
  // Subtract 3 hours
  const subtractedDate = subHours(dateTimeObject, 3);
  
  // Format the result
  const formattedSubtractedDate = format(subtractedDate, 'dd.MM.yyyy HH:mm:ss');
  
  return formattedSubtractedDate
};


export function CustomMap(currentDay, yesterday, branch) {

  let result;

  if (currentDay[branch] === null || yesterday[branch] === null) {
    result = ['null', 'null', 'null']
  } else {
    result = [currentDay[branch], yesterday[branch], Persents(currentDay[branch], yesterday[branch])]
  }
  return result
};


export function Persents(today, yesterday) {

    let percent = ((today-yesterday)/yesterday*100).toFixed(1)
    percent = percent.toString() + '%'
    percent = percent.replace('-', '');
    return percent
};


export const extractProperty = (dataList, key) => {
    return dataList.map(item => item[key]);
};


export const extractProperties = (dataList, propertyKey) => {
  return dataList.map(item => ({ dates: item.dates, [propertyKey]: item[propertyKey] }));
};


export function mapArrivedValues(data, dateArray, propertyKey) {
  const result = Array(dateArray.length).fill(null);

  data.forEach(item => {
    const index = dateArray.indexOf(item.dates);
    if (index !== -1) {
      result[index] = item[propertyKey];
    }
  });

  return result;
}

  
export function ensureArrayLength(array, desiredLength) {
  if (array.length !== desiredLength) {
    const numberOfTimesToInsert = desiredLength - array.length;

    for (let i = 0; i < numberOfTimesToInsert; i++) {
      array.unshift(null);
    }
  }
};

export function DeadTableProcess(dataset) {

  // Using map to transform each item in the dataset
  const modifiedObjects = dataset.map(item => {
    return {
      'ФИО': item.pat_fio,
      '№ ИБ': item.ib_num,
      'Пол': item.sex,
      'Возраст': item.age,
      'Отделение': item.dept,
      'Дата поступления': DateFormatting(item.arriving_dt),
      'Состояние при поступлении': item.state,
      'Кол-во койко дней': item.days,
      'Дигноз при поступлении': item.diag_arr,
      'Дигноз при выписке': item.diag_dead
    };
  });

  // Returning the array of modified objects
  return modifiedObjects;
};

export function ArrivedOarTable(dataset) {

  const modifiedObjects = dataset.map(item => {
    return {
      'ФИО': item.pat_fio,
      '№ ИБ': item.ib_num,
      'Возраст': item.age,
      'Отделение': item.dept,
      'Лечащий врач': item.doc_fio,
      'Дигноз при поступлении': item.diag_start,

    };
  });

  return modifiedObjects;
};


export function MovedOarTable(dataset) {

  const modifiedObjects = dataset.map(item => {
    return {
      'ФИО': item.pat_fio,
      '№ ИБ': item.ib_num,
      'Возраст': item.age,
      'Отделение': item.dept,
      'Лечащий врач': item.doc_fio,
      'Дигноз при поступлении': item.diag_start,
      'Дата перевода': item.move_date,
      'Переведен из': item.from_dept
    };
  });

  return modifiedObjects;
};

export function CurrentOarTable(dataset) {

  const modifiedObjects = dataset.map(item => {
    return {
      'ФИО': item.pat_fio,
      '№ ИБ': item.ib_num,
      'Возраст': item.age,
      'Отделение': item.dept,
      'Койко дней': item.days,
      'Лечащий врач': item.doc_fio,
      'Дигноз при поступлении': item.diag_start
    };
  });

  return modifiedObjects;
};


export function DeadsOarTable(dataset) {

  const modifiedObjects = dataset.map(item => {
    return {
      'ФИО': item.pat_fio,
      '№ ИБ': item.ib_num,
      'Пол': item.sex,
      'Возраст': item.age,
      'Отделение': item.dept,
      'Дата поступления': item.arriving_dt,
      'Состояние при поступлении': item.state,
      'Кол-во койко дней': item.days,
      'Дигноз при поступлении': item.diag_arr,
      'Дигноз при выписке': item.diag_dead
    };
  });

  return modifiedObjects;
};


export function GetNameOfDay(dateString) {
  return ['Вск', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб']
  [new Date(dateString).getDay()];
}



export function getYesterdayDate() {
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  return yesterday
}



export function getMainDMK(dmkData, day, nums) {

  let mainDMK;


  if (dmkData.length > 1 && dmkData[dmkData.length - nums]['dates'] === formatDate(day)) {
    mainDMK = dmkData[dmkData.length - nums];
  } else {
    mainDMK = { dates: formatDate(day), arrived: null, hosp: null, refused: null,
                signout: null, deads: null, reanimation: null };
  }
  
  return mainDMK;
}
