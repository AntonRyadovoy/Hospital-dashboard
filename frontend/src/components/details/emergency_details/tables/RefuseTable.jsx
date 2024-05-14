import React from 'react';
import { useTable } from 'react-table';

const refuseColumns = ['ФИО врача', 'кол-во отказов'];
const readyData = [['Поляков И.С.', 13], ['Иванова А.Н.', 8], ['Сидоров П.В.', 5]];

const RefuseTable = React.memo(({ onRowClick }) => {
  const columns = React.useMemo(() =>
    refuseColumns.map(column => ({
      Header: column,
      accessor: column,
    })),
    []
  );

  const data = React.useMemo(() =>
    readyData.map(dataRow => Object.fromEntries(refuseColumns.map((col, index) => [col, dataRow[index]]))),
    []
  );

  const {
    getTableProps,
    getTableBodyProps,
    headerGroups,
    rows,
    prepareRow,
  } = useTable({ columns, data });

  return (
    <>
      <span className='detail_block_header'> Список сгруппированных отказов </span>
      <table {...getTableProps()} className='table'>
        <thead>
          {headerGroups.map(headerGroup => (
            <tr {...headerGroup.getHeaderGroupProps()}>
              {headerGroup.headers.map(column => (
                <th {...column.getHeaderProps()}>{column.render('Header')}</th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody {...getTableBodyProps()}>
          {rows.map(row => {
            prepareRow(row);
            return (
              <tr
                {...row.getRowProps()}
                onClick={() => onRowClick(row.original['ФИО врача'])}
              >
                {row.cells.map(cell => (
                  <td {...cell.getCellProps()}>{cell.render('Cell')}</td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </>
  );
});

export default RefuseTable;
