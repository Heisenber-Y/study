

DROP TABLE  IF EXISTS DWD_GRAPHBASE.ACCOUNT_TO_PERSON_TMP1;
CREATE  TABLE DWD_GRAPHBASE.ACCOUNT_TO_PERSON_TMP1 AS (
select
 GUM.POLICYNO,
gp.businessno businessno ,-- 业务号
       gp.tradeno tradeno ,-- 交易流水号
       gb.bankaccountcode bankaccountcode ,-- 收款方银行账号
       gb.accountcname accountcname ,-- 收款方银行名称
        t.codecname as checkfeecheckind, -- 实名认证模式
       case
         when t2.certsource = 'zbx' and t2.paymenttype = '20' then
          '银行卡'
         when t2.certsource = 'zbx' and t2.paymenttype = '81' then
          '支付宝'
         when t2.certsource = 'zbx' and t2.paymenttype = '82' then
          '微信'
         when t2.certsource = 'zbx' and t2.paymenttype = '10' then
          '现金'
         when (t2.certsource is null or t2.certsource = 'js') and
              t2.paymenttype = '01' then
          'pos机刷卡'
         when (t2.certsource is null or t2.certsource = 'js') and
              t2.paymenttype = '02' then
          '支付宝'
         when (t2.certsource is null or t2.certsource = 'js') and
              t2.paymenttype = '03' then
          '微信'
         when (t2.certsource is null or t2.certsource = 'js') and
              t2.paymenttype = '04' then
          '个人转账'
         when (t2.certsource is null or t2.certsource = 'js') and
              t2.paymenttype = '07' then
          '现金'
         when (t2.certsource is null or t2.certsource = 'js') and
              t2.paymenttype = '09' then
          '企业转账'
         else
          t2.certsource || '-' || t2.paymenttype
       end payway ,-- 支付方式
       gp.payeename payeename ,--投保人名称
       t2.bankno bankno ,-- 付款方账号
       -- 投保人证件类型
       t3.codecname identifytypeall ,
       gt.identifynumber identifynumber --投保人证件号码
       ,gp.payfee 
       ,gp.paydate
	   	   ,"太财" COMPANY
      ,GUM.COMPANYCODE	--机构代码
  from (
  select * from DWD_GRAPHBASE.FILTER_GPPAYFEEINFO gp 
  where --gp.underwriteenddate > date '2020-06-21'    ---- UNDERWRITEENDDATE 核保完成日期
   gp.paymentno is not null
   and gp.businesstype in ('1', '2')) gp
   INNER JOIN 
   DWD_GRAPHBASE.FILTER_GUPOLICYMAIN GUM 
   ON GP.BUSINESSNO =GUM.PROPOSALNO
  left join DWD_GRAPHBASE.FILTER_GPTRADEINFO gt
    on gp.tradeno = gt.tradeno
    left outer join (select distinct t2.codecname,codecode from DWD_GRAPHBASE.FILTER_GGCODE t2 where t2.codetype='IdentifyTypeAll' ) t3 on t3.codecode=gt.identifytype
  left outer join  (select t.codecname,codecode
          from DWD_GRAPHBASE.FILTER_ggcode t
         where t.codetype = 'checkfeecheckind'
           ) t on t.codecode = gt.checkind
  left join DWD_GRAPHBASE.FILTER_gfbankaccount gb
    on gb.ofbankaccountcode = gt.bankaccountcode
  left join (select t1.tradeno,
                    t1.paymenttype,
                    t1.bankno,
                    t1.certsource,
                    row_number() over(partition by t1.tradeno order by t1.serialno desc) rn
               from DWD_GRAPHBASE.FILTER_gucitradeinfo t1
              where t1.authenticationresult in ('01', '02', '04','1') ) t2
    on t2.tradeno = gp.tradeno
   and rn = 1
 Where 1=1
   --And Gp.Underwriteenddate > Date '2020-06-21'
   And Gp.Paymentno Is Not Null
   And Gp.Businesstype In ('1', '2')
   );
   
   
   
--DROP TABLE  IF EXISTS DWD_GRAPHBASE.ACCOUNT_TO_PERSON_TMP2;
--CREATE  TABLE DWD_GRAPHBASE.ACCOUNT_TO_PERSON_TMP2 AS   

DROP TABLE  IF EXISTS DWD_GRAPHBASE.ACCOUNT_TO_PERSON_TMP2;
CREATE  TABLE DWD_GRAPHBASE.ACCOUNT_TO_PERSON_TMP2 AS (
SELECT 
CONCAT('P00000000',NVL(T1.PAYEENAME,'未知'),NVL(T1.IDENTIFYTYPEALL,'未知'),NVL(T1.IDENTIFYNUMBER,'未知')) AS PERSONID
,CONCAT('A00000000',BANKACCOUNTCODE) AS ACCOUNTNOID   
,T1.POLICYNO --保单号
,T1.BUSINESSNO ---- 业务号
,T1.TRADENO -- 交易流水号
,T1.BANKACCOUNTCODE -- 收款方银行账号
,T1.ACCOUNTCNAME -- 收款方银行名称
,T1.CHECKFEECHECKIND -- 实名认证模式
,T1.PAYWAY -- 支付方式 
,T1.PAYEENAME  --投保人名称
,T1.BANKNO -- 付款方账号
,T1.IDENTIFYTYPEALL  -- 投保人证件类型
,T1.IDENTIFYNUMBER  --投保人证件号码
,T1.PAYFEE  --金额
,T1.PAYDATE  --银行到账日期
,T2.MOBILEPHONE --投保人电话
,T2.ETL_DATE
,T1.COMPANY
,T1.COMPANYCODE
FROM 
DWD_GRAPHBASE.ACCOUNT_TO_PERSON_TMP1 T1
INNER JOIN 
DWD_GRAPHBASE.FILTER_GUPOLICYRELATEDPARTY T2 ON T1.POLICYNO = T2.POLICYNO 
WHERE 1=1
 AND T2.INSUREDCODE NOT IN (
  SELECT DISTINCT T.CLIENTCODE FROM dwd_graphbase.filter_gsclientcorporate T
  )

);

--获取个人到银行账号的关系
DROP TABLE  IF EXISTS DWD_GRAPHBASE.ACCOUNT_TO_PERSON_${DATE_CONF};
CREATE  TABLE DWD_GRAPHBASE.ACCOUNT_TO_PERSON_${DATE_CONF} AS (
SELECT 
T1.ACCOUNTNOID  
,T2.REGISTNO --报案号
,T1.PAYFEE SUMPAID --金额
,T1.BANKACCOUNTCODE ACCOUNTNO  
,T1.MOBILEPHONE PAYEEMOBILE
,T1.ACCOUNTCNAME  PAYEEBANKACCOUNTNAME -- 收款方账户名称
,T1.ACCOUNTCNAME PAYEEBANKNAME --领赔款单位/代理人/索赔人
,T1.PAYWAY  PAIDMETHODCODE --赔款支付方式
,'投保人'  PAYEETYPE --赔款接收人类型
,T1.ETL_DATE ETL_DATE
,'ACCOUNT_OF_PERSON_OWN' KIND
,T1.COMPANYCODE COMPANYCODE_ACCOUNT
,T1.COMPANY COMPANY_ACCOUNT
,'' as ACCOUNT_LAST_MARKER
,'' AS ACCOUNT_MARKED_STATUS
,'' AS ACCOUNT_MARKED_TAG
,'1990-01-01  00:00:00' AS ACCOUNT_LAST_MARKED_TIME
,T1.PERSONID --人实体ID
,T1.POLICYNO --保单号码
,'' AS ENTERPRISE_KIND_NAME
,T1.PAYEENAME AS USERNAME
,T1.IDENTIFYTYPEALL IDENTIFYTYPE
,T1.IDENTIFYNUMBER  IDENTIFYNUMBER
,T1.MOBILEPHONE AS TEL
,'ACCOUNT_OF_PERSON_OWN' AS KIND_1
,T1.ETL_DATE ETL_DATE_1
,T1.COMPANYCODE COMPANYCODE_PERSON
,T1.COMPANY COMPANY_PERSON
,'' as PERSON_LAST_MARKER
,'' AS PERSON_MARKED_STATUS
,'' AS PERSON_MARKED_TAG
,'1990-01-01  00:00:00' AS PERSON_LAST_MARKED_TIME
,'ACCOUNT_OF_PERSON_OWN'  AS RELATIONSHIP
, T1.ACCOUNTNOID SRCID
, T1.PERSONID DESID
FROM 
DWD_GRAPHBASE.ACCOUNT_TO_PERSON_TMP2 T1
LEFT JOIN 
DWD_GRAPHBASE.FILTER_GCCLAIMMAIN T2
ON T1.POLICYNO=T2.POLICYNO
WHERE T1.PERSONID IS NOT NULL
AND T1.ACCOUNTNOID IS NOT NULL 
);

