--账号实体：
DROP TABLE  IF EXISTS DWD_GRAPHBASE.ACCOUNT_ENTITY_TMP1;
CREATE  TABLE DWD_GRAPHBASE.ACCOUNT_ENTITY_TMP1 AS (
--WITH ACCOUNT_ENTITY AS (
SELECT 
 CONCAT('A','00000000',T.ACCOUNTNO) ACCOUNTNOID
 ,T1.REGISTNO
,T.SUMPAID--赔付金额
,T.ACCOUNTNO --银行账号
,T.PAYEEMOBILE --收款人手机号
,T.PAYEEBANKACCOUNTNAME  --收款方账户名称
,T.PAYEEBANKNAME --开户行 PAYEEBANKNAME
,T.PAIDMETHODCODE  --赔款支付方式
,T.PAYEETYPE  --赔款接收人类型
,T.ETL_DATE 
,CASE WHEN T.PAYEETYPE='002' THEN 'ACCOUNT_INSURED'  WHEN T.PAYEETYPE='011' THEN 'ACCOUNT_THIRD_PARTY'   ELSE NULL END  KIND	
,T1.COMPANYCODE	--机构代码
,"太财" COMPANY	
FROM dwd_graphbase.filter_gcadjustmentfee T 
LEFT OUTER JOIN 
dwd_graphbase.filter_gcclaimmain T1
ON T.CLAIMNO=T1.CLAIMNO
WHERE 1=1 
AND T.PAYEETYPE IN('002','011','005') --002是被保人，011 是三者 ，005 是公估行
AND T.PAIDMETHODCODE='212' --212 是银行转账， 211是支票 ，111 是现金
);

--保单对应的银行账号
DROP TABLE  IF EXISTS DWD_GRAPHBASE.ACCOUNT_POLICYNO_TMP1;
CREATE  TABLE DWD_GRAPHBASE.ACCOUNT_POLICYNO_TMP1 AS (
SELECT
 CONCAT('A','00000000',T1.PAYEEBANKACCOUNT) ACCOUNTNOID
 ,T3.REGISTNO --报案号
 ,T2.SUMGROSSPREMIUM  SUMPAID --总保费
 ,T1.PAYEEBANKACCOUNT  ACCOUNTNO --银行账号
 ,T1.PAYEEMOBILE  --手机号码
 ,T1.PAYEEACCOUNTNAME PAYEEBANKACCOUNTNAME --收款方账户名称
 ,T1.PAYEEBANKNAME  --开户行
,T1.PAYEECOMCODE  PAIDMETHODCODE --收款方机构号
,T1.PAYEETYPE --收款人类型
,T1.ETL_DATE
,'ACCOUNT_OF_POLICYNO' AS KIND
,T3.COMPANYCODE	--机构代码
,"太财" COMPANY	
FROM 
dwd_graphbase.filter_GPPOLICYTRANSINFO T1 --事实表_太财退保帐户信息
LEFT JOIN 
dwd_graphbase.filter_gupolicymain T2
ON T1.POLICYNO=T2.POLICYNO
LEFT JOIN 
dwd_graphbase.filter_gcclaimmain T3
ON T1.POLICYNO=T3.POLICYNO
);


--个人或者企业的付款银行账号

DROP TABLE IF EXISTS DWD_GRAPHBASE.ACCOUNT_PAY_TMP1 ;
CREATE TABLE DWD_GRAPHBASE.ACCOUNT_PAY_TMP1 AS 
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
   ;


DROP TABLE IF EXISTS DWD_GRAPHBASE.ACCOUNT_PAY_TMP2 ;
CREATE TABLE DWD_GRAPHBASE.ACCOUNT_PAY_TMP2 AS    
SELECT 
CONCAT('A00000000',BANKACCOUNTCODE) AS ACCOUNTNOID   
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
,T1.COMPANYCODE COMPANYCODE
,T1.COMPANY COMPANY
FROM 
DWD_GRAPHBASE.ACCOUNT_PAY_TMP1 T1
LEFT JOIN 
DWD_GRAPHBASE.FILTER_GCCLAIMMAIN T2
ON T1.POLICYNO=T2.POLICYNO
;
   
   


--企业对应的银行账号：



DROP TABLE  IF EXISTS DWD_GRAPHBASE.ACCOUNT_UNION_TMP1;
CREATE  TABLE DWD_GRAPHBASE.ACCOUNT_UNION_TMP1 AS (
SELECT 
ACCOUNTNOID
,REGISTNO
,SUMPAID
,ACCOUNTNO
,PAYEEMOBILE
,PAYEEBANKACCOUNTNAME
,PAYEEBANKNAME
,PAIDMETHODCODE
,PAYEETYPE
,ETL_DATE
,KIND
,COMPANYCODE
,COMPANY
FROM 
DWD_GRAPHBASE.ACCOUNT_ENTITY_TMP1
UNION
SELECT 
ACCOUNTNOID
,REGISTNO
,SUMPAID
,ACCOUNTNO
,PAYEEMOBILE
,PAYEEBANKACCOUNTNAME
,PAYEEBANKNAME
,PAIDMETHODCODE
,PAYEETYPE
,ETL_DATE
,KIND
,COMPANYCODE
,COMPANY
FROM 
DWD_GRAPHBASE.ACCOUNT_POLICYNO_TMP1
UNION
 SELECT 
ACCOUNTNOID
,REGISTNO
,SUMPAID
,ACCOUNTNO
,PAYEEMOBILE
,PAYEEBANKACCOUNTNAME
,PAYEEBANKNAME
,PAIDMETHODCODE
,PAYEETYPE
,ETL_DATE
,KIND
,COMPANYCODE
,COMPANY
FROM 
DWD_GRAPHBASE.ACCOUNT_PAY_TMP2

);


DROP TABLE  IF EXISTS DWD_GRAPHBASE.ACCOUNT_ORG;
CREATE  TABLE DWD_GRAPHBASE.ACCOUNT_ORG AS 
SELECT
POLICYNOID,
concat(concat('@',concat_ws('@',collect_set(companycode))),'@') AS COMPANY_CODE_ORG
FROM 
dwd_graphbase.ACCOUNT_UNION_TMP1 
GROUP BY 
ACCOUNTNOID
;






--DROP TABLE  IF EXISTS DWD_GRAPHBASE.ACCOUNT_ENTITY;
--INSERT OVERWRITE TABLE DWD_GRAPHBASE.ACCOUNT_ENTITY
DROP TABLE  IF EXISTS DWD_GRAPHBASE.ACCOUNT_ENTITY;
CREATE  TABLE DWD_GRAPHBASE.ACCOUNT_ENTITY AS 
SELECT DISTINCT
T.ACCOUNTNOID
,T.ACCOUNTNO
,T.SUMPAID
,T.REGISTNO
,T.PAYEEMOBILE
,T.PAYEEBANKACCOUNTNAME
,T.PAYEEBANKNAME
,T.PAIDMETHODCODE
,T.PAYEETYPE
,T.ETL_DATE
,T.KIND
,T.COMPANYCODE
,T.COMPANY
,T2.COMPANY_CODE_ORG
,'' as ACCOUNT_LAST_MARKER
,'' AS ACCOUNT_MARKED_STATUS
,'' AS ACCOUNT_MARKED_TAG
,'1990-01-01  00:00:00' AS ACCOUNT_LAST_MARKED_TIME
FROM 
DWD_GRAPHBASE.ACCOUNT_UNION_TMP1 T
LEFT JOIN 
DWD_GRAPHBASE.ACCOUNT_ORG T2 
ON T.ACCOUNTNOID=T2.ACCOUNTNOID
WHERE T.ACCOUNTNOID IS NOT NULL
;












