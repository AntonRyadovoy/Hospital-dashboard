import React, { useState, useContext } from "react";
import { useSpring, animated } from "react-spring";
import DeadsChartDetail from "./DeadsChartDetail";
import SignOutDetailTable from "./SignOutDetailTable";
import DataContext from "../../DataContext";
import '../signIn_detail_board/detail_blocks.css';



const SignOutDetailBoard = () => {

  const data = useContext(DataContext);
  let dmk_current = data.dmk
  dmk_current = dmk_current[dmk_current.length - 1]
  let kis = data.kis
  kis = kis[0].signout[0]


  const [isDeadTableVisible, setIsDeadTableVisible] = useState(false);
  const toggleDeadTableVisibility = () => {
    setIsDeadTableVisible(!isDeadTableVisible);
  };

  // Spring effect ofappearance
  const springProps = useSpring({
    transform: `scale(${isDeadTableVisible ? 1 : 0})`,
    height: isDeadTableVisible ? "auto" : 0,
    opacity: isDeadTableVisible ? 1 : 0,
    config: { tension: 200, friction: 25 },
    delay: isDeadTableVisible ? 30 : 0,
    width: '-webkit-fill-available',
  });

  return (
    <div className='detail_block'>
      <span className='detail_block_header'> Исходы </span>
      <div className='blocks_container'>
        <div className='separated_detail_block_X' onClick={toggleDeadTableVisibility}>
          <p> Смерть </p> { kis.deads }
        </div>
        <div className='separated_detail_block'> 
          <p> Перевод в другую МО </p> { kis.moved } </div>
        <div className='separated_detail_block'>
           <p> Выписка </p> { kis.signout }
       </div>
      </div>
      <span className='detail_block_header'> Выписка по отделениям </span>
      <DeadsChartDetail data={kis}/>
      <animated.div style={springProps}>
          {isDeadTableVisible && <SignOutDetailTable />}
      </animated.div>
    </div>
  );
};

export default SignOutDetailBoard;
