import React, { useContext, useEffect } from 'react';
import { useTable } from 'react-table';
import ScaleX from '../ScaleX';
import DataContext from '../../DataContext';


const SignInDetailTable = () => {

  let data = JSON.parse(sessionStorage.getItem('data')).dmk.accum_dmk

  const columns = [
    { Header: 'Профиль', accessor: 'name' },
    {
      Header: 'План',
      accessor: 'fact',
      Cell: ({ cell, row }) => (<ScaleX hospFact={cell.value} hospPlan={row.original.plan} />), 
    },
  ];

  // Create a table instance
  const { getTableProps, getTableBodyProps, headerGroups, rows, prepareRow } = useTable({
    columns,
    data, // Use the provided data directly
  });

  
  // Use useEffect to trigger table update when data changes
  useEffect(() => {
    // Trigger update of the table instance when 'data' changes
    // This is necessary to reflect the changes in the table
    prepareRow(rows);
  }, [data, prepareRow, rows]);

  return (
    <div className='detail_block_header'>
      ПЛАН/ФАКТ по профилям
      <table className='signin-table' {...getTableProps()}>
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
              <tr {...row.getRowProps()}>
                {row.cells.map(cell => (
                  <td {...cell.getCellProps()}>{cell.render('Cell')}</td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

export default SignInDetailTable;
