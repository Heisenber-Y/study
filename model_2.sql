<transform table_name="slc_person" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			select
			  distinct
				a.TOPACTUALID,
				a.CERTIFICATENO,
				a.CERTIFICATETYPE,
				a.NAME,
				a.TEL,
				a.KIND,
				a.EMAIL,
				a.ADDRESS,
				a.CREATEDATE
			from djt_generateid_person a
			where a.CERTIFICATETYPE in ('1','15') AND a.KIND in ('REPORTER','ACCIDENTDRIVER')
			]]>
		</transform>

		<transform table_name="slc_similar_conditions_with_notificationtel" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
		  select
		    distinct
				a.ACCIDENTLOCATION,
				c.CERTIFICATENO,
				a.NOTIFICATIONTEL,
				d.PLATENO,
				d.GARAGE_NAME,
				count(distinct a.CASEFOLDERID) as ALL_CNT, --同地，出险时驾驶员、报案电话、牌照号码、估损单维修厂名称均相同的案件数统计
				0 as ACCIDENTDRIVER_CNT, --同地，同一出险时驾驶员的案件数统计
				0 as NOTIFICATIONTEL_CNT --同地，同一报案电话的案件数统计
			from djt_generateid_valuablecase a 
			join slc_person c 
			on a.CASEFOLDERID=c.TOPACTUALID and c.KIND='ACCIDENTDRIVER' -- 报案人身份证都为空，不再保存到人员表
			join djt_generateid_vehicle d 
			on a.CASEFOLDERID=d.TOPACTUALID 
			where a.ACCIDENTLOCATION is not NULL
			group by a.ACCIDENTLOCATION, c.CERTIFICATENO, a.NOTIFICATIONTEL, d.PLATENO, d.GARAGE_NAME 
			having count(distinct a.CASEFOLDERID) > 2
			]]>
		</transform>
	
		<!-- rule 2 -->
		<transform table_name="slc_similar_conditions_with_accidentdriver_rule_two" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
		    select 
				distinct
				a.ACCIDENTLOCATION,
				c.CERTIFICATENO,
				0 as ALL_CNT, --同地，出险时驾驶员、报案电话、牌照号码、估损单维修厂名称均相同的案件数统计
				count(distinct a.CASEFOLDERID) as ACCIDENTDRIVER_CNT, --同地，同一出险时驾驶员的案件数统计
				0 as NOTIFICATIONTEL_CNT --同地，同一报案电话的案件数统计
			from djt_generateid_valuablecase a  
			join slc_person c 
			on a.CASEFOLDERID=c.TOPACTUALID and c.KIND='ACCIDENTDRIVER' 
			join djt_generateid_vehicle d 
			on a.CASEFOLDERID=d.TOPACTUALID and d.THIRDPARTYFLAG='N' and d.PLATENO is not null
			where a.ACCIDENTLOCATION is not NULL
			group by a.ACCIDENTLOCATION, c.CERTIFICATENO
			having count(distinct a.CASEFOLDERID) > 3 and count(distinct d.PLATENO) > 1
			]]>
		</transform>

		<transform table_name="slc_similar_conditions_with_notificationtel_rule_two" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
		    select 
				distinct
				a.ACCIDENTLOCATION,
				a.NOTIFICATIONTEL,
				0 as ALL_CNT, --同地，出险时驾驶员、报案电话、牌照号码、估损单维修厂名称均相同的案件数统计
				0 as ACCIDENTDRIVER_CNT, --同地，同一出险时驾驶员的案件数统计
				count(distinct a.CASEFOLDERID) as NOTIFICATIONTEL_CNT --同地，同一报案电话的案件数统计
			from djt_generateid_valuablecase a 
			join djt_generateid_vehicle d 
			on a.CASEFOLDERID=d.TOPACTUALID and d.THIRDPARTYFLAG='N' and d.PLATENO is not null
			where a.ACCIDENTLOCATION is not NULL
			group by a.ACCIDENTLOCATION, a.NOTIFICATIONTEL 
			having count(distinct a.CASEFOLDERID) > 3 and count(distinct d.PLATENO) > 1
			]]>
		</transform>

		<transform table_name="slc_preprocessing_suspect_case" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			select
			  a.ACCIDENTLOCATION,
			  max(a.ALL_CNT) as ALL_CNT,
			  max(a.ACCIDENTDRIVER_CNT) as ACCIDENTDRIVER_CNT,
			  max(a.NOTIFICATIONTEL_CNT) as NOTIFICATIONTEL_CNT
			from
			(
			  select 
					distinct
					a.ACCIDENTLOCATION,
					e.ALL_CNT, --同地，出险时驾驶员、报案电话、牌照号码、估损单维修厂名称均相同的案件数统计
					e.ACCIDENTDRIVER_CNT, --同地，同一出险时驾驶员的案件数统计
					e.NOTIFICATIONTEL_CNT --同地，同一报案电话的案件数统计
				from djt_generateid_valuablecase a 
				join slc_person c 
				on a.CASEFOLDERID=c.TOPACTUALID and c.KIND='ACCIDENTDRIVER'
				join djt_generateid_vehicle d 
				on a.CASEFOLDERID=d.TOPACTUALID 
				join slc_similar_conditions_with_notificationtel e 
				on e.ACCIDENTLOCATION=a.ACCIDENTLOCATION and e.CERTIFICATENO=c.CERTIFICATENO 
				and e.NOTIFICATIONTEL=a.NOTIFICATIONTEL and e.PLATENO=d.PLATENO and e.GARAGE_NAME=d.GARAGE_NAME
      	
				union all
      	
		  	select 
					distinct
					a.ACCIDENTLOCATION,
					e.ALL_CNT, --同地，出险时驾驶员、报案电话、牌照号码、估损单维修厂名称均相同的案件数统计
					e.ACCIDENTDRIVER_CNT, --同地，同一出险时驾驶员的案件数统计
					e.NOTIFICATIONTEL_CNT --同地，同一报案电话的案件数统计 
				from djt_generateid_valuablecase a 
				join slc_person c 
				on a.CASEFOLDERID=c.TOPACTUALID and c.KIND='ACCIDENTDRIVER'
				join slc_similar_conditions_with_accidentdriver_rule_two e 
				on e.ACCIDENTLOCATION=a.ACCIDENTLOCATION and e.CERTIFICATENO=c.CERTIFICATENO
				
				union all
      	
				select 
					distinct
					a.ACCIDENTLOCATION,
					e.ALL_CNT, --同地，出险时驾驶员、报案电话、牌照号码、估损单维修厂名称均相同的案件数统计
					e.ACCIDENTDRIVER_CNT, --同地，同一出险时驾驶员的案件数统计
					e.NOTIFICATIONTEL_CNT --同地，同一报案电话的案件数统计 
				from djt_generateid_valuablecase a 
				join slc_similar_conditions_with_notificationtel_rule_two e 
				on e.ACCIDENTLOCATION=a.ACCIDENTLOCATION and e.NOTIFICATIONTEL=a.NOTIFICATIONTEL
			) a group by a.ACCIDENTLOCATION
			]]>
		</transform>

		<transform table_name="slc_suspect_case" type="sql" >
			<![CDATA[
		  select 
			  distinct 
				a.DAJAID,
				a.CASEFOLDERID,
				a.NOTIFICATIONNO,
				a.NOTIFICATIONDATE,
	      a.NOTIFICATIONTEL,
				a.ACCIDENTCAUSE,
				a.ACCIDENTDATE,
				a.ACCIDENTLOCATION,

				--新增字段
				a.TEL_DAJAID,
				a.NOTIFICATIONNAME,
				a.UNDERWIRTING_BRANCH_CODE,
				a.UNDERWIRTING_BRANCH_NAME,
				a.ACCIDENT_BRANCH_CODE,
				a.ACCIDENT_BRANCH_NAME,
				a.SURVEYOR_BRANCH_CODE,
				a.SURVEYOR_BRANCH_NAME,
				a.ASSESSMENT_BRANCH_CODE,
				a.ASSESSMENT_BRANCH_NAME,
				a.ASSESSMENTSTAFF_CODE,
				a.ASSESSMENTSTAFF_NAME,
				a.ISINJURYINVOLVED,
				a.CLAIMAMOUNT,

				a.TYPE,
				a.IS_DELETE,
				a.IS_RANDOM,
				a.ACCIDENTLOCATION AS rule_name,
				
			  b.ALL_CNT, --同地，出险时驾驶员、报案电话、牌照号码、估损单维修厂名称均相同的案件数统计
				b.ACCIDENTDRIVER_CNT, --同地，同一出险时驾驶员的案件数统计
				b.NOTIFICATIONTEL_CNT --同地，同一报案电话的案件数统计 
			from djt_generateid_valuablecase a
			join slc_preprocessing_suspect_case b
			on a.ACCIDENTLOCATION = b.ACCIDENTLOCATION
			]]>
		</transform>
		<!--************************************************* 模型1：同地多案 *************************************************-->   
		
		<!--************************************************* 模型2：同号多案 *************************************************-->   
		<transform table_name="stc_matched_telephone" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			  select
			  	a.NOTIFICATIONTEL,
			  	max(a.arg1) as PLATENO_CNT,
			  	max(a.arg2) as BRANCH_CNT
			  from
			  (
			    select 
		        distinct 
			    	a.NOTIFICATIONTEL,
			    	count(distinct a.CASEFOLDERID) as arg1,
			    	0 as arg2
			    from djt_generateid_valuablecase a 
			   join djt_generateid_vehicle b on a.CASEFOLDERID=b.TOPACTUALID 
			    where a.NOTIFICATIONTEL IS NOT NULL
			    group by a.NOTIFICATIONTEL 
			    having count(distinct a.CASEFOLDERID) > 5 and count(distinct b.PLATENO) > 1
			    
			    UNION ALL
		      
		      select 
		      	distinct 
			  	  a.NOTIFICATIONTEL,
			  	  0 as arg1,
			  	  count(distinct a.CASEFOLDERID) as arg2
			    from djt_generateid_valuablecase a 
			    where a.NOTIFICATIONTEL IS NOT NULL
			    group by a.NOTIFICATIONTEL  
			    having count(distinct a.CASEFOLDERID) > 3 and count(distinct a.UNDERWIRTING_BRANCH_CODE) > 1
			  ) a group by a.NOTIFICATIONTEL
			]]>
		</transform>

		<transform table_name="stc_suspect_case" type="sql"  >
			<![CDATA[
		    select 
			  distinct 
				a.DAJAID,
				a.CASEFOLDERID,
				a.NOTIFICATIONNO,
				a.NOTIFICATIONDATE,                                                                          
	      a.NOTIFICATIONTEL,
				a.ACCIDENTCAUSE,
				a.ACCIDENTDATE,
				a.ACCIDENTLOCATION,

				--新增字段
				a.TEL_DAJAID,
				a.NOTIFICATIONNAME,
				a.UNDERWIRTING_BRANCH_CODE,
				a.UNDERWIRTING_BRANCH_NAME,
				a.ACCIDENT_BRANCH_CODE,
				a.ACCIDENT_BRANCH_NAME,
				a.SURVEYOR_BRANCH_CODE,
				a.SURVEYOR_BRANCH_NAME,
				a.ASSESSMENT_BRANCH_CODE,
				a.ASSESSMENT_BRANCH_NAME,
				a.ASSESSMENTSTAFF_CODE,
				a.ASSESSMENTSTAFF_NAME,
				a.ISINJURYINVOLVED,
				a.CLAIMAMOUNT,

				a.TYPE,
				a.IS_DELETE,
				a.IS_RANDOM,
				a.NOTIFICATIONTEL AS rule_name,
				
				b.PLATENO_CNT,  --同一“报案电话”，不同车辆报案次数
				b.BRANCH_CNT  --同一“报案电话”，在不同理赔公司中报案次数
			from djt_generateid_valuablecase a 
			join stc_matched_telephone b 
				on a.NOTIFICATIONTEL=b.NOTIFICATIONTEL
			]]>
		</transform>
		<!--************************************************* 模型2：同号多案 *************************************************-->   
		
		<!--************************************************* 模型3：同车多案 *************************************************-->   
	<!-- 变更 20170614: 车的唯一性标识改为车牌，VIN替换为PLATENO -->
	<transform table_name="svc_person" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			  select
			  	a.TOPACTUALID,
			  	a.CERTIFICATENO,
			  	a.CERTIFICATETYPE,
			  	a.NAME,
			  	a.TEL,
			  	a.KIND,
			  	a.EMAIL,
			  	a.ADDRESS,
			  	a.CREATEDATE   
			  from djt_generateid_person a  
			  where a.KIND in ('REPORTER','ACCIDENTDRIVER')
			]]>
		</transform>

		<transform table_name="svc_suspect_plateno" type="sql"  transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			  select
			  	b.PLATENO,
			  	count(distinct c.CERTIFICATENO) as PERSON_NUM,
			  	count(distinct b.TOPACTUALID) as ACCIDENTDRIVER_CNT
			  from djt_generateid_valuablecase a 
			  join djt_generateid_vehicle b 
			    on a.CASEFOLDERID=b.TOPACTUALID and b.PLATENO is not null
			  join svc_person c 
			    on a.CASEFOLDERID=c.TOPACTUALID and c.KIND = 'ACCIDENTDRIVER' --报案人身份证都为空，不再保存到人员表
			  join djt_generateid_insurance d 
			    on a.CASEFOLDERID=d.TOPACTUALID and d.POLICYNO is NOT NULL
			  group by b.PLATENO,d.POLICYNO having count(distinct c.CERTIFICATENO) > 1 and count(distinct b.TOPACTUALID) > 3
			]]>
		</transform>

		<transform table_name="svc_suspect_plateno_topactualid" type="sql"  transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			  select
			  	a.TOPACTUALID,
			  	b.PLATENO,
			  	b.ACCIDENTDRIVER_CNT
			  from djt_generateid_vehicle a
			  join svc_suspect_plateno b
			    on a.PLATENO=b.PLATENO
			]]>
		</transform>

		<transform table_name="svc_suspect_case" type="sql" >
			<![CDATA[
			select 
			  distinct 
				a.DAJAID,
				a.CASEFOLDERID,
				a.NOTIFICATIONNO,
				a.NOTIFICATIONDATE,
	            a.NOTIFICATIONTEL,
				a.ACCIDENTCAUSE,
				a.ACCIDENTDATE,
				a.ACCIDENTLOCATION,

				--新增字段
				a.TEL_DAJAID,
				a.NOTIFICATIONNAME,
				a.UNDERWIRTING_BRANCH_CODE,
				a.UNDERWIRTING_BRANCH_NAME,
				a.ACCIDENT_BRANCH_CODE,
				a.ACCIDENT_BRANCH_NAME,
				a.SURVEYOR_BRANCH_CODE,
				a.SURVEYOR_BRANCH_NAME,
				a.ASSESSMENT_BRANCH_CODE,
				a.ASSESSMENT_BRANCH_NAME,
				a.ASSESSMENTSTAFF_CODE,
				a.ASSESSMENTSTAFF_NAME,
				a.ISINJURYINVOLVED,
				a.CLAIMAMOUNT,

				a.TYPE,
				a.IS_DELETE,
				a.IS_RANDOM,
				b.PLATENO,
				b.PLATENO AS rule_name,
				b.ACCIDENTDRIVER_CNT  --同一“车牌号”车辆一个“出险年度”内,且报案人或驾驶员不是同一人的案件的报案次数
			from djt_generateid_valuablecase a  
			join svc_suspect_plateno_topactualid b
			  on a.CASEFOLDERID=b.TOPACTUALID
			]]>
		</transform>
		<!--************************************************* 模型3：同车多案 *************************************************-->   
		
		<!--************************************************* 模型4：疑似倒签单 *************************************************-->   
		<transform table_name="suspect_antidatedinsurance_cases" type="sql" >  <!-- suspect: 可疑; AntiDated: 倒签 -->
			<![CDATA[
			SELECT
			  DISTINCT
			  a.DAJAID,
				a.CASEFOLDERID,
				a.NOTIFICATIONNO,
				a.NOTIFICATIONDATE,                         
		    a.NOTIFICATIONTEL,
				a.ACCIDENTCAUSE,
				a.ACCIDENTDATE,
				a.ACCIDENTLOCATION,

				--新增字段
				a.TEL_DAJAID,
				a.NOTIFICATIONNAME,
				a.UNDERWIRTING_BRANCH_CODE,
				a.UNDERWIRTING_BRANCH_NAME,
				a.ACCIDENT_BRANCH_CODE,
				a.ACCIDENT_BRANCH_NAME,
				a.SURVEYOR_BRANCH_CODE,
				a.SURVEYOR_BRANCH_NAME,
				a.ASSESSMENT_BRANCH_CODE,
				a.ASSESSMENT_BRANCH_NAME,
				a.ASSESSMENTSTAFF_CODE,
				a.ASSESSMENTSTAFF_NAME,
				a.ISINJURYINVOLVED,
				a.CLAIMAMOUNT,

				a.TYPE,
				a.IS_DELETE,
				a.IS_RANDOM,
				b.POLICYNO AS rule_name,  --规则名: 保单号
				DATEDIFF(a.ACCIDENTDATE, b.EFFECTIVEDATE) as EFFECTIVEDATE_CNT,  --出险时间至保单起期天数
				DATEDIFF(b.EXPIREDATE, a.ACCIDENTDATE) as EXPIREDATE_CNT  --出险时间至保单到期天数
			FROM djt_generateid_valuablecase a
			  JOIN djt_generateid_insurance b
			    ON a.CASEFOLDERID = b.TOPACTUALID 
			    AND a.ACCIDENTDATE IS NOT NULL AND b.EFFECTIVEDATE IS NOT NULL AND b.EXPIREDATE IS NOT NULL
			WHERE (DATEDIFF(a.ACCIDENTDATE, b.EFFECTIVEDATE) >= 0 AND DATEDIFF(a.ACCIDENTDATE, b.EFFECTIVEDATE) < 15)     --起始 15 天内
				OR (DATEDIFF(b.EXPIREDATE, a.ACCIDENTDATE) >= 0 AND DATEDIFF(b.EXPIREDATE, a.ACCIDENTDATE) < 7) 	 	   --终止 7 天内
			]]>
		</transform>
		<!--************************************************* 模型4：疑似倒签单 *************************************************-->   
		
		<!--************************************************* 模型5：可疑碰瓷车 *************************************************-->   
		<!-- 变更 20170614: 车的唯一性标识改为车牌，VIN替换为PLATENO -->
		<transform table_name="srcc_case_to_thirdpart" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			SELECT
			  DISTINCT
	  		a.DAJAID,
				a.CASEFOLDERID,
				a.NOTIFICATIONNO,
				a.NOTIFICATIONDATE,                                      
		    a.NOTIFICATIONTEL,
				a.ACCIDENTCAUSE,
				a.ACCIDENTDATE,
				a.ACCIDENTLOCATION,

				--新增字段
				a.TEL_DAJAID,
				a.NOTIFICATIONNAME,
				a.UNDERWIRTING_BRANCH_CODE,
				a.UNDERWIRTING_BRANCH_NAME,
				a.ACCIDENT_BRANCH_CODE,
				a.ACCIDENT_BRANCH_NAME,
				a.SURVEYOR_BRANCH_CODE,
				a.SURVEYOR_BRANCH_NAME,
				a.ASSESSMENT_BRANCH_CODE,
				a.ASSESSMENT_BRANCH_NAME,
				a.ASSESSMENTSTAFF_CODE,
				a.ASSESSMENTSTAFF_NAME,
				a.ISINJURYINVOLVED,
				a.CLAIMAMOUNT,

				a.TYPE,
				a.IS_DELETE,
				a.IS_RANDOM,
	  	  SUBSTR(b.DAJAID, 10, LENGTH(b.DAJAID)) AS ID --车架号或车牌
			FROM djt_generateid_valuablecase a
	  		JOIN djt_generateid_vehicle b
	  			ON a.CASEFOLDERID = b.TOPACTUALID AND b.THIRDPARTYFLAG='Y'
	  			  AND NOTIFICATIONNO IS NOT NULL
	  			  AND b.DAJAID != 'V00000000未知'
	  		]]>
		</transform>

		<!-- 统计同车作为三者车出险2次以上的所有车牌号及其关联案件 -->
		<transform table_name="suspect_racketeercar_cases" type="sql" > <!-- suspect: 可疑; racketeer car: 碰瓷车 -->
			<![CDATA[
			SELECT
			  DISTINCT
				b.DAJAID,
				b.CASEFOLDERID,
				b.NOTIFICATIONNO,
				b.NOTIFICATIONDATE,                                      
		    b.NOTIFICATIONTEL,
				b.ACCIDENTCAUSE,
				b.ACCIDENTDATE,
				b.ACCIDENTLOCATION,

				--新增字段
				b.TEL_DAJAID,
				b.NOTIFICATIONNAME,
				b.UNDERWIRTING_BRANCH_CODE,
				b.UNDERWIRTING_BRANCH_NAME,
				b.ACCIDENT_BRANCH_CODE,
				b.ACCIDENT_BRANCH_NAME,
				b.SURVEYOR_BRANCH_CODE,
				b.SURVEYOR_BRANCH_NAME,
				b.ASSESSMENT_BRANCH_CODE,
				b.ASSESSMENT_BRANCH_NAME,
				b.ASSESSMENTSTAFF_CODE,
				b.ASSESSMENTSTAFF_NAME,
				b.ISINJURYINVOLVED,
				b.CLAIMAMOUNT,

				b.TYPE,
				b.IS_DELETE,
				b.IS_RANDOM,
				b.ID AS rule_name,   	--规则名: 车架号或车牌
				a.CNT as THIRDPARTY_CNT  --同一车辆作为三者车出险次数
			FROM
			(
				SELECT ID, COUNT(DISTINCT NOTIFICATIONNO) AS cnt
				  FROM srcc_case_to_thirdpart
				GROUP BY ID HAVING COUNT(DISTINCT NOTIFICATIONNO) > 2
			 ) a JOIN srcc_case_to_thirdpart b ON a.ID = b.ID
			]]>
		</transform>
		<!--************************************************* 模型5：可疑碰瓷车 *************************************************-->   
		
		<!--************************************************* 合并模型，分组 *************************************************-->   
		<!-- 第一步_1，处理同地多案 -->
		<transform table_name="regrouping_cases_step_1_slc_suspect_case" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			SELECT
			  s.NOTIFICATIONNO as NODE_ID,  --节点ID
			  t.SUB_GRAPH_ID,  --所属子图ID
			  t.REPRESENT_NODE_ID,  --代表节点ID
			  "1" as MODEL_ID,  --所属模型编号
			  s.CLAIMAMOUNT as CLAIM_AMOUNT,  --案件赔付金额
			  s.ALL_CNT as ARG1,  --参数一：同地，出险时驾驶员、报案电话、牌照号码、估损单维修厂名称均相同的案件数统计
			  s.ACCIDENTDRIVER_CNT as ARG2,  --参数二：同地，同一出险时驾驶员的案件数统计
			  s.NOTIFICATIONTEL_CNT as ARG3,  --参数三：同地，同一报案电话的案件数统计
			  '' as ARG4,  --参数四：预留字段
			  '' as ARG5  --参数五：预留字段
			FROM slc_suspect_case s
			JOIN 
			(
			  SELECT
			    a.ACCIDENTLOCATION,
			    a.SUB_GRAPH_ID,
			    substr(a.SUB_GRAPH_ID,21,18) as REPRESENT_NODE_ID  --找到最小时间的报案号作为代表节点ID
			  FROM 
			  (
			    --按出险地点对模型分子图，找到最小时间作为子图ID
			    SELECT
			    	ACCIDENTLOCATION,
			    	--拼接报案时间和报案号，比较大小时从左往右对比，其实质还是在比较报案时间
			    	--这样做能防止报案时间重复但报案号不一样的情况出现
			    	min(concat(NOTIFICATIONDATE,'@',NOTIFICATIONNO)) as SUB_GRAPH_ID
			    FROM slc_suspect_case
			    GROUP BY ACCIDENTLOCATION
			  ) a
			) t ON s.ACCIDENTLOCATION = t.ACCIDENTLOCATION
	    ]]>
		</transform>
		<!-- 第一步_2，处理同号多案 -->
		<transform table_name="regrouping_cases_step_1_stc_suspect_case" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			SELECT
			  s.NOTIFICATIONNO as NODE_ID,  --节点ID
			  t.SUB_GRAPH_ID,  --所属子图ID
			  t.REPRESENT_NODE_ID,  --代表节点ID
			  "2" as MODEL_ID,  --所属模型编号
			  s.CLAIMAMOUNT as CLAIM_AMOUNT,  --案件赔付金额
			  s.PLATENO_CNT as ARG1,  --参数一：同一“报案电话”，不同车辆报案次数
			  s.BRANCH_CNT as ARG2,  --参数二：同一“报案电话”，在不同理赔公司中报案次数
			  '' as ARG3,  --参数三：预留字段
			  '' as ARG4,  --参数四：预留字段
			  '' as ARG5  --参数五：预留字段
			FROM stc_suspect_case s
			JOIN 
			(
			  SELECT
			    a.NOTIFICATIONTEL,
			    a.SUB_GRAPH_ID,
			    substr(a.SUB_GRAPH_ID,21,18) as REPRESENT_NODE_ID  --找到最小时间的报案号作为代表节点ID
			  FROM
			  (
			    --按报案电话对模型分子图，找到最小时间作为子图ID
			    SELECT
			      NOTIFICATIONTEL,
			    	--拼接报案时间和报案号，比较大小时从左往右对比，其实质还是在比较报案时间
			    	--这样做能防止报案时间重复但报案号不一样的情况出现
			    	min(concat(NOTIFICATIONDATE,'@',NOTIFICATIONNO)) as SUB_GRAPH_ID
			    FROM stc_suspect_case
			    GROUP BY NOTIFICATIONTEL
			  ) a
			) t ON s.NOTIFICATIONTEL = t.NOTIFICATIONTEL
	    ]]>
		</transform>
		<!-- 第一步_3，处理同车多案 -->
		<!-- 变更 20170614: 车的唯一性标识改为车牌，VIN替换为PLATENO -->
		<transform table_name="regrouping_cases_step_1_svc_suspect_case" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			SELECT
			  s.NOTIFICATIONNO as NODE_ID,  --节点ID
			  t.SUB_GRAPH_ID,  --所属子图ID
			  t.REPRESENT_NODE_ID,  --代表节点ID
			  "3" as MODEL_ID,  --所属模型编号
			  s.CLAIMAMOUNT as CLAIM_AMOUNT,  --案件赔付金额
			  s.ACCIDENTDRIVER_CNT as ARG1,  --参数一：同一“车牌号”号码车辆一个“出险年度”内,且报案人或驾驶员不是同一人的案件的报案次数
			  '' as ARG2,  --参数二：预留字段
			  '' as ARG3,  --参数三：预留字段
			  '' as ARG4,  --参数四：预留字段
			  '' as ARG5  --参数五：预留字段
			FROM svc_suspect_case s
			JOIN 
			(
			  SELECT
			    a.PLATENO,
			    a.SUB_GRAPH_ID,
			    substr(a.SUB_GRAPH_ID,21,18) as REPRESENT_NODE_ID  --找到最小时间的报案号作为代表节点ID
			  FROM 
			  (
			    --按车牌号对模型分子图，找到最小时间作为子图ID
			   	SELECT
			   	  PLATENO,
			    	--拼接报案时间和报案号，比较大小时从左往右对比，其实质还是在比较报案时间
			    	--这样做能防止报案时间重复但报案号不一样的情况出现
			    	min(concat(NOTIFICATIONDATE,'@',NOTIFICATIONNO)) as SUB_GRAPH_ID
			   	FROM svc_suspect_case
			   GROUP BY PLATENO
			  ) a
			) t ON s.PLATENO = t.PLATENO
	    ]]>
		</transform>
		<!-- 第一步_4，处理疑似倒签单 -->
		<transform table_name="regrouping_cases_step_1_suspect_antidatedinsurance_cases" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			SELECT
			  s.NOTIFICATIONNO as NODE_ID,  --节点ID
			  --拼接报案时间和报案号，比较大小时从左往右对比，其实质还是在比较报案时间
			  --这样做能防止报案时间重复但报案号不一样的情况出现
			  concat(s.NOTIFICATIONDATE,'@',s.NOTIFICATIONNO) as SUB_GRAPH_ID, --所属子图ID
			  s.NOTIFICATIONNO as REPRESENT_NODE_ID,  --代表节点ID
			  "4" as MODEL_ID,  --所属模型编号
			  s.CLAIMAMOUNT as CLAIM_AMOUNT,  --案件赔付金额
			  s.EFFECTIVEDATE_CNT as ARG1,  --参数一：出险时间至保单起期天数
			  s.EXPIREDATE_CNT as ARG2,  --参数二：出险时间至保单到期天数
			  '' as ARG3,  --参数三：预留字段
			  '' as ARG4,  --参数四：预留字段
			  '' as ARG5  --参数五：预留字段
			FROM suspect_antidatedinsurance_cases s
	    ]]>
		</transform>
		<!-- 第一步_5，处理可疑碰瓷车 -->
		<transform table_name="regrouping_cases_step_1_suspect_racketeercar_cases" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			SELECT
			  s.NOTIFICATIONNO as NODE_ID,  --节点ID
			  --拼接报案时间和报案号，比较大小时从左往右对比，其实质还是在比较报案时间
			  --这样做能防止报案时间重复但报案号不一样的情况出现
			  concat(s.NOTIFICATIONDATE,'@',s.NOTIFICATIONNO) as SUB_GRAPH_ID, --所属子图ID
			  s.NOTIFICATIONNO as REPRESENT_NODE_ID,  --代表节点ID
			  "5" as MODEL_ID,  --所属模型编号
			  s.CLAIMAMOUNT as CLAIM_AMOUNT,  --案件赔付金额
			  s.THIRDPARTY_CNT as ARG1,  --参数一：同一车辆作为三者车出险次数
			  '' as ARG2,  --参数二：预留字段
			  '' as ARG3,  --参数三：预留字段
			  '' as ARG4,  --参数四：预留字段
			  '' as ARG5   --参数五：预留字段
			FROM suspect_racketeercar_cases s
	    ]]>
		</transform>

		<!-- 第二步，合并各个模型的所有子图 -->
		<transform table_name="regrouping_cases_step_2" type="sql" transform_mode="overwrite" localpath="./external" >
			<![CDATA[
			SELECT
			  t2.NODE_ID,
			  t2.SUB_GRAPH_ID,
			  t2.REPRESENT_NODE_ID,
			  t2.MODEL_ID,
			  t2.CLAIM_AMOUNT,
			  t2.ARG1,
			  t2.ARG2,
			  t2.ARG3,
			  t2.ARG4,
			  t2.ARG5
			FROM
			(
			  SELECT
			    t.NODE_ID,
			    t.SUB_GRAPH_ID,
			    t.REPRESENT_NODE_ID,
			    t.MODEL_ID,
			    t.CLAIM_AMOUNT,
			    max(t.ARG1) as ARG1,
			    max(t.ARG2) as ARG2,
			    max(t.ARG3) as ARG3,
			    max(t.ARG4) as ARG4,
			    max(t.ARG5) as ARG5,
			    row_number() over (partition by t.NODE_ID,t.MODEL_ID order by t.SUB_GRAPH_ID asc) rank
			  FROM
			  (
			    SELECT
			      NODE_ID,
			      SUB_GRAPH_ID,
			      REPRESENT_NODE_ID,
			      MODEL_ID,
			      CLAIM_AMOUNT,
			      ARG1,
			      ARG2,
			      ARG3,
			      ARG4,
			      ARG5
			    FROM regrouping_cases_step_1_slc_suspect_case
			    
			    UNION ALL
			    
			    SELECT
			      NODE_ID,
			      SUB_GRAPH_ID,
			      REPRESENT_NODE_ID,
			      MODEL_ID,
			      CLAIM_AMOUNT,
			      ARG1,
			      ARG2,
			      ARG3,
			      ARG4,
			      ARG5
			    FROM regrouping_cases_step_1_stc_suspect_case
			    
			    UNION ALL
			    
			    SELECT
			      NODE_ID,
			      SUB_GRAPH_ID,
			      REPRESENT_NODE_ID,
			      MODEL_ID,
			      CLAIM_AMOUNT,
			      ARG1,
			      ARG2,
			      ARG3,
			      ARG4,
			      ARG5
			    FROM regrouping_cases_step_1_svc_suspect_case
			    
			    UNION ALL
			    
			    SELECT
			      NODE_ID,
			      SUB_GRAPH_ID,
			      REPRESENT_NODE_ID,
			      MODEL_ID,
			      CLAIM_AMOUNT,
			      ARG1,
			      ARG2,
			      ARG3,
			      ARG4,
			      ARG5
			    FROM regrouping_cases_step_1_suspect_antidatedinsurance_cases
			    
			    UNION ALL
			    
			    SELECT
			      NODE_ID,
			      SUB_GRAPH_ID,
			      REPRESENT_NODE_ID,
			      MODEL_ID,
			      CLAIM_AMOUNT,
			      ARG1,
			      ARG2,
			      ARG3,
			      ARG4,
			      ARG5
			    FROM regrouping_cases_step_1_suspect_racketeercar_cases
			  ) t GROUP BY t.NODE_ID, t.SUB_GRAPH_ID, t.REPRESENT_NODE_ID, t.MODEL_ID, t.CLAIM_AMOUNT
			) t2 WHERE t2.rank = 1
	    ]]>
		</transform>
		
		<!-- 第三步，找到所有交叉子图中代表节点的最小子图ID -->
		<transform table_name="regrouping_cases_step_3" type="sql" transform_mode="overwrite" localpath="${PIPE_EXTERNAL_PATH}" >
			<![CDATA[
			--根据代表节点分组，找到该代表节点对应的子图ID的最小值
			SELECT
			  c.REPRESENT_NODE_ID,
			  min(c.MIN_SUB_GRAPH_ID) as MIN_SUB_GRAPH_ID
			FROM
			(
			  --找到包含公共节点的所有子图，且子图节点不是最小值的代表节点ID（可能出现代表节点一样，但子图ID不一样的多条记录）
			  SELECT
			    a.REPRESENT_NODE_ID,
			    b.MIN_SUB_GRAPH_ID
			  FROM regrouping_cases_step_2 a
			  JOIN (
			    --找到同时属于多个子图的节点,和包含该节点的最小子图ID
			    SELECT
			      NODE_ID as PUBLIC_NODE_ID,
			      min(SUB_GRAPH_ID) as MIN_SUB_GRAPH_ID
			    FROM regrouping_cases_step_2
			    GROUP BY NODE_ID
			    HAVING COUNT(1) > 1
			  ) b ON a.NODE_ID = b.PUBLIC_NODE_ID AND a.SUB_GRAPH_ID != b.MIN_SUB_GRAPH_ID
			) c GROUP BY c.REPRESENT_NODE_ID
	    ]]>
		</transform>
	    
	  <!-- 第四步，合并最小子图ID，重新分组 -->
		<transform table_name="regrouping_cases_step_4" type="sql" transform_mode="overwrite" localpath="./external" >
			<![CDATA[
			SELECT
			  a.NODE_ID,
			  CASE WHEN b.MIN_SUB_GRAPH_ID IS NOT NULL AND b.MIN_SUB_GRAPH_ID < a.SUB_GRAPH_ID THEN b.MIN_SUB_GRAPH_ID
			  	ELSE a.SUB_GRAPH_ID END as SUB_GRAPH_ID,
			  CASE WHEN a.MODEL_ID='1' THEN '1'
			       WHEN a.MODEL_ID='2' THEN '2'
			       WHEN a.MODEL_ID='3' THEN '4'
			       WHEN a.MODEL_ID='4' THEN '8'
			       WHEN a.MODEL_ID='5' THEN '16' END AS MODEL_ID,  --模型编号，取2的n-1次方
			  CASE WHEN a.MODEL_ID='1' THEN '同地多案'
			       WHEN a.MODEL_ID='2' THEN '同号多案'
			       WHEN a.MODEL_ID='3' THEN '同车多案'
			       WHEN a.MODEL_ID='4' THEN '疑似倒签单'
			       WHEN a.MODEL_ID='5' THEN '可疑碰瓷车' END AS MODEL_NAME,  --模型名称
			  a.CLAIM_AMOUNT,
			  a.ARG1,
			  a.ARG2,
			  a.ARG3,
			  a.ARG4,
			  a.ARG5
			FROM regrouping_cases_step_2 a
			LEFT JOIN regrouping_cases_step_3 b ON a.REPRESENT_NODE_ID = b.REPRESENT_NODE_ID
			]]>
		</transform>
		<transform table_name="regrouping_suspect_cases" type="sql" >
			<![CDATA[
			SELECT
			  a.NODE_ID,
			  CASE WHEN b.SUB_GRAPH_ID is not null THEN b.SUB_GRAPH_ID
			       ELSE a.SUB_GRAPH_ID END as SUB_GRAPH_ID,
			  a.MODEL_ID,
			  a.MODEL_NAME,
			  a.CLAIM_AMOUNT,
			  a.ARG1,
			  a.ARG2,
			  a.ARG3,
			  a.ARG4,
			  a.ARG5
			FROM regrouping_cases_step_4 a
			LEFT JOIN
			(
			  SELECT
			    NODE_ID,
			    min(SUB_GRAPH_ID) AS SUB_GRAPH_ID
			  FROM regrouping_cases_step_4
			  GROUP BY NODE_ID
			) b ON a.NODE_ID = b.NODE_ID
			]]>
		</transform>
	    
	  <!-- 第五步，导出最后结果为xsv文件(共2个)，方便mysql导入 -->
		<transform table_name="regrouping_suspect_cases_result_2" type="sql" >
			<![CDATA[
			SELECT
			  a.NODE_ID AS NOTIFICATIONNO,
			  MD5(a.SUB_GRAPH_ID) AS GROUP_ID,
			  a.MODEL_ID,
			  a.CLAIM_AMOUNT,
			  a.ARG1,
			  a.ARG2,
			  a.ARG3,
			  a.ARG4,
			  a.ARG5
			FROM regrouping_suspect_cases a
			JOIN 
			(
			  SELECT
			    NODE_ID,
			    COUNT(*) AS CNTS
			  FROM regrouping_suspect_cases
			  GROUP BY NODE_ID
			) b ON a.NODE_ID = b.NODE_ID AND b.CNTS > 1  --同时满足多个模型
			JOIN
			(
				SELECT
				  SUB_GRAPH_ID,
				  COUNT(distinct NODE_ID) AS CNTS
				FROM regrouping_suspect_cases
				GROUP BY SUB_GRAPH_ID
			) c ON a.SUB_GRAPH_ID = c.SUB_GRAPH_ID AND c.CNTS < 100  --单个分组小于100
			]]>
		</transform>
		<transform table_name="regrouping_suspect_cases_result_1" type="sql" >
			<![CDATA[
			SELECT
			  a.NOTIFICATIONNO,
			  a.GROUP_ID,
			  a.CLAIM_AMOUNT,
			  CAST(CAST(SUM(a.MODEL_ID) AS DECIMAL(10,0)) AS STRING) AS MODELS
			FROM
			(
			  SELECT
			    NOTIFICATIONNO,
			    GROUP_ID,
			    MODEL_ID,
			    CLAIM_AMOUNT
			  FROM regrouping_suspect_cases_result_2
			) a GROUP BY a.NOTIFICATIONNO, a.GROUP_ID, a.CLAIM_AMOUNT
			]]>
		</transform>
		<!--************************************************* 合并模型，分组 *************************************************-->   
	</transforms>
