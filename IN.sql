SELECT 
   '利润表行集' AS REPORT_NAME,
   A.CURRENCY,
   A.LEDGER_ID,
   A.SEGMENT1,
   A.STAT_MONTH,
   A.PERIOD_YEAR,
   A.PERIOD_NAME,
   'A210' AS AXIS_SEQ,
   '短EB（非IGP非自保）手续费及佣金支出' AS DESCRIPTION,
   SUM((NVL(A.PERIOD_NET_DR, 0) - NVL(A.PERIOD_NET_CR, 0))) AS  CUR_VALUE, --本期值
   SUM((NVL(A.BEGIN_BALANCE_DR, 0) - NVL(A.BEGIN_BALANCE_CR, 0) +
        NVL(A.PERIOD_NET_DR, 0) - NVL(A.PERIOD_NET_CR, 0))) AS  END_OF_PERIOD, --期末余额
   SUM((NVL(A.BEGIN_BALANCE_DR, 0) - NVL(A.BEGIN_BALANCE_CR, 0))) AS  BEGIN_OF_PERIOD, --期初余额
   from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as ETL_TIME
from EBS_FACT_EBS_GL_DATA_M  A
WHERE 1=1
    AND A.STAT_MONTH='201812'
    AND A.ledger_id = 2023 --太养账套ID
    and A.segment2 like '6421%'
   and (A.segment4 in 
   (select FVV.FLEX_VALUE --段值头表
          from dlk_tpg000_faods.EBS_FND_FLEX_VALUES_VL               fvv,
               dlk_tpg000_faods.EBS_FND_FLEX_VALUE_NORM_HIERARCHY ffh
         where fvv.FLEX_VALUE_SET_ID = ffh.flex_value_set_id
           and fvv.FLEX_VALUE_SET_ID = 1016850 --产品段_ID
           and fvv.FLEX_VALUE between ffh.child_flex_value_low and
               ffh.child_flex_value_high
           and ffh.parent_flex_value  in ('22K10','22J10','22L11','22L31')) 
           AND A.segment4 NOT IN ('22J10132', '22J10141', '22J10171'))
   and A.segment8  not in 
   (select FVV.FLEX_VALUE --段值头表
          from dlk_tpg000_faods.EBS_FND_FLEX_VALUES_VL               fvv,
               dlk_tpg000_faods.EBS_FND_FLEX_VALUE_NORM_HIERARCHY ffh
         where fvv.FLEX_VALUE_SET_ID = ffh.flex_value_set_id
           and fvv.FLEX_VALUE_SET_ID = 1016854 --行业专用段_ID
           and fvv.FLEX_VALUE between ffh.child_flex_value_low and
               ffh.child_flex_value_high
           and ffh.parent_flex_value IN ('229901','229902'))
GROUP BY A.LEDGER_ID,
    A.SEGMENT1,
    A.STAT_MONTH,
    A.PERIOD_YEAR,
    A.PERIOD_NAME,
    A.CURRENCY
