select 
				DISTINCT
				nrm.CLAIMNO, --立案号
				nrm.CAPPNO, --报案号
				ncl.CLASSCODE, --险种代码
				nrm.ESTAMT, --预估金额
				nrm.BRANCHID, --分公司代码
				ncl.PRECODE, --保障责任代码
				ncl.POLICYNO, --保单号
				nrm.pid,  	--被保人取理赔立案信息表
				nrm.modate --结案时间
			from 
				NLP_NREGCLM nrm --理赔立案信息表
			inner  join 
				NLP_NCLMCCL ncl --理赔计算项
			on 
				nrm.CLAIMNO =  ncl.CLAIMNO --根据立案号关联
			inner join NLP_NCLMAPP mapp
				on nrm.cappno=mapp.cappno
			where (
				length(REGEXP_REPLACE(REGEXP_REPLACE(nvl(nrm.name,'未知'),' ',''),' ',''))<5 
				or (
				length(REGEXP_REPLACE(REGEXP_REPLACE(nvl(nrm.name,'未知'),' ',''),' ',''))>=5 
				and nvl(nrm.name,'未知') not like '%合作%'
				and nvl(nrm.name,'未知') not like '%社%'
				and nvl(nrm.name,'未知') not like '%银行%'
				and nvl(nrm.name,'未知') not like '%公司%'
				and nvl(nrm.name,'未知') not like '%集团%'
				and nvl(nrm.name,'未知') not like '%中心%'
				and nvl(nrm.name,'未知') not like '%院%'
				and nvl(nrm.name,'未知') not like '%房%'
				and nvl(nrm.name,'未知') not like '%室%'
				and nvl(nrm.name,'未知') not like '%管理%'
				and nvl(nrm.name,'未知') not like '%办%'
				and nvl(nrm.name,'未知') not like '%厂%'
				and nvl(nrm.name,'未知') not like '%会%'
				and nvl(nrm.name,'未知') not like '%校%'
				and nvl(nrm.name,'未知') not like '%公安%'
				and nvl(nrm.name,'未知') not like '%保险%'
				and nvl(nrm.name,'未知') not like '%互保%'
				and nvl(nrm.name,'未知') not like '%化工%'
				and nvl(nrm.name,'未知') not like '%局%'
				and nvl(nrm.name,'未知') not like '%大队%'
				and nvl(nrm.name,'未知') not like '%处%'
				and nvl(nrm.name,'未知') not like '%政府%')
				)

			]]>
		</transform>
		
		
		<transform table_name="middle2" type="sql" transform_mode="overwrite"  >
			<![CDATA[
 			 select
				middle1.CLAIMNO, --立案号
				middle1.PRECODE, --保障责任代码
				clo.PID, --领款人证件号码
				clo.CLMSETNO, --帐号
				clo.mphone, --领款人手机号  取领款人联系方式 
				middle1.CLASSCODE, --险种代码
				middle1.ESTAMT, --预估金额
				middle1.BRANCHID, --分公司代码
				middle1.MODATE, --结案时间
				nvl(rin.appname,ncu2.name) name,
				middle1.policyno   --保单号
			from 
				middle1
			inner  join 
				NLP_CLMSETNO clo --领款人信息表
			 on 
				middle1.CAPPNO =  clo.CAPPNO --根据立案号关联
			inner join
				NLP_RISKCON rin --保单信息表	
			on 
				rin.policyno = middle1.policyno --根据保单号关联
			and rin.CLASSCODE=middle1.CLASSCODE
			and 	rin.cappno=middle1.CAPPNO
			left JOIN 
				BLACK_TEL bt
			on clo.mphone=bt.tel
			left join 
				NLP_NCUSTMATL ncu2 --客户信息表
			on
				rin.apid=ncu2.id --id主键关联
			 where  
				clo.PID <> middle1.PID --领款人非被保险人
				and bt.tel is null
				and clo.mphone is not null
			HAVING (
			length(REGEXP_REPLACE(REGEXP_REPLACE(nvl(name,'未知'),' ',''),' ',''))<5 
			or (
			length(REGEXP_REPLACE(REGEXP_REPLACE(nvl(name,'未知'),' ',''),' ',''))>=5 
			and nvl(name,'未知') not like '%合作%'
			and nvl(name,'未知') not like '%社%'
			and nvl(name,'未知') not like '%银行%'
			and nvl(name,'未知') not like '%公司%'
			and nvl(name,'未知') not like '%集团%'
			and nvl(name,'未知') not like '%中心%'
			and nvl(name,'未知') not like '%院%'
			and nvl(name,'未知') not like '%房%'
			and nvl(name,'未知') not like '%室%'
			and nvl(name,'未知') not like '%管理%'
			and nvl(name,'未知') not like '%办%'
			and nvl(name,'未知') not like '%厂%'
			and nvl(name,'未知') not like '%会%'
			and nvl(name,'未知') not like '%校%'
			and nvl(name,'未知') not like '%公安%'
			and nvl(name,'未知') not like '%保险%'
			and nvl(name,'未知') not like '%互保%'
			and nvl(name,'未知') not like '%化工%'
			and nvl(name,'未知') not like '%局%'
			and nvl(name,'未知') not like '%大队%'
			and nvl(name,'未知') not like '%处%'
			and nvl(name,'未知') not like '%政府%')
			)

			]]>
		</transform>
		
		<transform table_name="middle2_1" type="sql" transform_mode="overwrite"  >
			<![CDATA[
 			 select
				middle1.CLAIMNO, --立案号
				middle1.PRECODE, --保障责任代码
				clo.PID, --领款人证件号码
				clo.CLMSETNO, --帐号
				clo.BENTELE, --领款人手机号  取领款人联系方式 
				middle1.CLASSCODE, --险种代码
				middle1.ESTAMT, --预估金额
				middle1.BRANCHID, --分公司代码
				middle1.MODATE, --结案时间
				nvl(rin.appname,ncu2.name) name,
				middle1.policyno   --保单号
			from 
				middle1
			inner  join 
				NLP_CLMSETNO clo --领款人信息表
			 on 
				middle1.CAPPNO =  clo.CAPPNO --根据立案号关联
			inner join
				NLP_RISKCON rin --保单信息表	
			on 
				rin.policyno = middle1.policyno --根据保单号关联
			and rin.CLASSCODE=middle1.CLASSCODE
			and 	rin.cappno=middle1.CAPPNO
			left JOIN 
				BLACK_TEL bt
			on clo.BENTELE=bt.tel
			left join 
				NLP_NCUSTMATL ncu2 --客户信息表
			on
				rin.apid=ncu2.id --id主键关联
			 where  
				clo.PID <> middle1.PID --领款人非被保险人
				and bt.tel is null
				and clo.BENTELE is not null
			HAVING (
			length(REGEXP_REPLACE(REGEXP_REPLACE(nvl(name,'未知'),' ',''),' ',''))<5 
			or (
			length(REGEXP_REPLACE(REGEXP_REPLACE(nvl(name,'未知'),' ',''),' ',''))>=5 
			and nvl(name,'未知') not like '%合作%'
			and nvl(name,'未知') not like '%社%'
			and nvl(name,'未知') not like '%银行%'
			and nvl(name,'未知') not like '%公司%'
			and nvl(name,'未知') not like '%集团%'
			and nvl(name,'未知') not like '%中心%'
			and nvl(name,'未知') not like '%院%'
			and nvl(name,'未知') not like '%房%'
			and nvl(name,'未知') not like '%室%'
			and nvl(name,'未知') not like '%管理%'
			and nvl(name,'未知') not like '%办%'
			and nvl(name,'未知') not like '%厂%'
			and nvl(name,'未知') not like '%会%'
			and nvl(name,'未知') not like '%校%'
			and nvl(name,'未知') not like '%公安%'
			and nvl(name,'未知') not like '%保险%'
			and nvl(name,'未知') not like '%互保%'
			and nvl(name,'未知') not like '%化工%'
			and nvl(name,'未知') not like '%局%'
			and nvl(name,'未知') not like '%大队%'
			and nvl(name,'未知') not like '%处%'
			and nvl(name,'未知') not like '%政府%')
			)

			]]>
		</transform>
		
		<transform table_name="middle3" type="sql" transform_mode="overwrite">
		<![CDATA[
  			select
				middle2.CLAIMNO, --立案号
				middle2.PID, --领款人证件号码
				middle2.CLMSETNO, --帐号
				middle2.mphone, --手机号
				middle2.CLASSCODE, --险种代码
				pre.ACCKIND, --索赔事故类型
				middle2.ESTAMT, --预估金额
				middle2.BRANCHID, --分公司代码
				middle2.MODATE, --结案时间
				middle2.policyno   --保单号
			from 
				middle2 
			inner join 
				NLP_PRECODE pre --原子责任的代码表
			on 
				middle2.PRECODE = pre.PRECODE --根据保障责任代码关联
				
			
			where 
				pre.ACCKIND in ('01','02','03')   --索赔事故类型 01死亡  02伤残  03重大疾病
			]]>
		</transform>
		
		<transform table_name="middle3_1" type="sql" transform_mode="overwrite">
		<![CDATA[
  			select
				middle2_1.CLAIMNO, --立案号
				middle2_1.PID, --领款人证件号码
				middle2_1.CLMSETNO, --帐号
				middle2_1.BENTELE, --手机号
				middle2_1.CLASSCODE, --险种代码
				pre.ACCKIND, --索赔事故类型
				middle2_1.ESTAMT, --预估金额
				middle2_1.BRANCHID, --分公司代码
				middle2_1.MODATE, --结案时间
				middle2_1.policyno   --保单号
			from 
				middle2_1
			inner join 
				NLP_PRECODE pre --原子责任的代码表
			on 
				middle2_1.PRECODE = pre.PRECODE --根据保障责任代码关联
				
			
			where 
				pre.ACCKIND in ('01','02','03')   --索赔事故类型 01死亡  02伤残  03重大疾病
			]]>
		</transform>
		
		<transform table_name="middle4" type="sql" transform_mode="overwrite">
			<![CDATA[
 			select
				middle3.CLAIMNO, --立案号
				middle3.PID, --领款人证件号码
				middle3.CLMSETNO, --帐号
				middle3.mphone, --手机号
				middle3.ACCKIND, --索赔事故类型
				middle3.ESTAMT, --预估金额
				middle3.BRANCHID, --分公司代码
				cm68.CLASSNAME, --险种名称
				middle3.MODATE, --结案时间
				middle3.policyno  --保单号
			from 
				middle3 
			inner  join 
				NLP_CLASSCODE_MATCH68 cm68 --6，8位险种代码转换表
			on 
				middle3.CLASSCODE = cm68.CLASSCODE6 --险种代码=6位的险种代码

			]]>
		</transform>
		
			<transform table_name="middle4_1" type="sql" transform_mode="overwrite">
			<![CDATA[
 			select
				middle3_1.CLAIMNO, --立案号
				middle3_1.PID, --领款人证件号码
				middle3_1.CLMSETNO, --帐号
				middle3_1.BENTELE, --手机号
				middle3_1.ACCKIND, --索赔事故类型
				middle3_1.ESTAMT, --预估金额
				middle3_1.BRANCHID, --分公司代码
				cm68.CLASSNAME, --险种名称
				middle3_1.MODATE, --结案时间
				middle3_1.policyno  --保单号
			from 
				middle3_1
			inner  join 
				NLP_CLASSCODE_MATCH68 cm68 --6，8位险种代码转换表
			on 
				middle3_1.CLASSCODE = cm68.CLASSCODE6 --险种代码=6位的险种代码

			]]>
		</transform>
		
		<!--去除对应规则其他多余字段-->
		 	
		<transform table_name="middle6" type="sql" transform_mode="overwrite">
			<![CDATA[
 				select 
					distinct(CLAIMNO), --立案号
					mphone, --手机号
					BRANCHID --分公司代码
					
				from 
					middle4
					where mphone is not null
			]]>
		</transform>
		 
		 <transform table_name="middle6_1" type="sql" transform_mode="overwrite">
			<![CDATA[
 				select 
					distinct(CLAIMNO), --立案号
					BENTELE, --手机号
					BRANCHID --分公司代码
					
				from 
					middle4_1
					where BENTELE is not null
			]]>
		</transform>
	 
	
		<!--************************************************* 模型2：同一电话多案领款 *************************************************-->   
		
		<transform table_name="temp2" type="sql" transform_mode="overwrite">
			<![CDATA[
				SELECT 
					mphone, --手机号
					count(mphone) AS SUM_mphone --以手机号计数
				FROM 
					middle6
				GROUP BY 
					mphone --以手机号分组
			]]>
		</transform>

		<transform table_name="temp2_1" type="sql" transform_mode="overwrite">
			<![CDATA[
				SELECT 
					BENTELE, --手机号
					count(BENTELE) AS SUM_BENTELE --以手机号计数
				FROM 
					middle6_1
				GROUP BY 
					BENTELE --以手机号分组
			]]>
		</transform>
		
		<transform table_name="sxlp_phone_money" type="sql" transform_mode="overwrite">
			<![CDATA[
				SELECT 
					middle4.CLAIMNO, --立案号
					middle4.mphone, --领款人手机号
					middle4.BRANCHID, --分公司代码
					temp2.SUM_mphone, --领款次数
					middle4.MODATE, --结案时间
					middle4.policyno
				FROM 
					middle4
				LEFT JOIN 
					temp2
				ON 
					middle4.mphone = temp2.mphone --根据领款人手机号关联
				WHERE 
					temp2.SUM_mphone>=2 --统计领款次数>=()天的
			]]>
		</transform>
		
		<transform table_name="sxlp_phone_money_1" type="sql" transform_mode="overwrite">
			 <![CDATA[
				SELECT 
					middle4_1.CLAIMNO, --立案号
					middle4_1.bentele, --领款人手机号
					middle4_1.BRANCHID, --分公司代码
					temp2_1.SUM_BENTELE, --领款次数
					middle4_1.MODATE, --结案时间
					middle4_1.policyno
				FROM 
					middle4_1
				LEFT JOIN 
					temp2_1
				ON 
					middle4_1.bentele = temp2_1.bentele --根据领款人手机号关联
				WHERE 
					temp2_1.SUM_BENTELE>=2 --统计领款次数>=()天的
			]]>
		</transform>
		 
       <transform table_name="temp2_2" type="sql" transform_mode="overwrite">
			 <![CDATA[
			 SELECT 
					CLAIMNO, --立案号
					mphone as bentele, --领款人手机号
					BRANCHID, --分公司代码
					MODATE, --结案时间
					policyno
				FROM 
					sxlp_phone_money
			 union
				SELECT 
					CLAIMNO, --立案号
					bentele, --领款人手机号
					BRANCHID, --分公司代码
					MODATE, --结案时间
					policyno
				FROM 
					sxlp_phone_money_1 
			]]>
		</transform>
		
		<transform table_name="temp2_3" type="sql" transform_mode="overwrite">
			 <![CDATA[
			 SELECT 
			        bentele,
					count(distinct CLAIMNO) as sum1	--立案号 
				FROM 
					temp2_2 
				group by bentele 
			]]>
		</transform>
		
		<transform table_name="temp2_4" type="sql" transform_mode="overwrite">
			 <![CDATA[
			 SELECT 
			       distinct 
			        temp2_2.CLAIMNO, --立案号
					temp2_2.bentele, --领款人手机号
					temp2_2.BRANCHID, --分公司代码
					temp2_2.MODATE, --结案时间
					temp2_2.policyno,
					temp2_3.sum1
				FROM 
					temp2_2 
					left join 
					temp2_3
					on temp2_2.bentele=temp2_3.bentele
			]]>
		</transform>
		
		
		
		<!-- 模型2数据用于导入mysl --> 
		<transform table_name="model_2_cases" type="sql" transform_mode="persist">
			<![CDATA[ 
					select 
					distinct
					--'1' AS MODEL_ID, --模型id
					concat(bentele,'-','2') AS GROUP_ID,	--组号
					CLAIMNO AS NOTIFICATIONNO, --赔案号
					'2' AS RULE_ID, --规则ID
					'' AS NOTIFICATIONDATE, --报案时间
					BRANCHID AS UNDERWIRTING_BRANCH_NAME, --出险分公司
					'' AS GARAGE_NAME, --厂商
					'' AS CLAIM_AMOUNT, --理赔金额			
					sum1 AS ARG1,  --参数1   出现次数
					'' AS ARG2,
					'' AS ARG3,
					'' AS ARG4,
					'' AS ARG5,
					MODATE, --结案时间
					'' as ARG6 ,
					'' as ARG7 ,
					'' as ARG8 
				from 
					temp2_4
			]]>
		</transform>
	
		 
		<transform table_name="model_2_status" type="sql">
			<![CDATA[
				select 
					GROUP_ID,	--组号
					NOTIFICATIONNO, --赔案号
					RULE_ID --规则ID
				from 
					model_2_cases 
			]]>
		</transform>
		
		<transform table_name="model_2_list" type="sql">
			<![CDATA[ 
			select 
				CLAIMNO,
				policyno
			from 
				temp2_4		
			]]>
		</transform>
		
	</transforms>
