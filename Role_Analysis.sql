CREATE VOLATILE TABLE vt_population AS
(
SELECT
	Client_ID
	, Agent_ID
	, Role_Type

FROM 		Client_table as t01

LEFT JOIN	Agent_table as t02
ON		t01.Agent_ID=t02.Agent_ID

QUALIFY ROW_NUMBER() OVER(PARTITION BY Client_ID, Agent_ID ORDER BY Role_Type DESC)=1
) WITH DATA
PRIMARY INDEX (Client_ID)
ON COMMIT PRESERVE ROWS
;


CREATE VOLATILE TABLE vt_agent_details_1 AS
(
SELECT
	pop.Client_ID
	, pop.Year
	, t01.Agent_ID
	, t01.Agent_Name

FROM 		vt_population as pop

LEFT JOIN	DB.Agent_store as t01
ON		pop.Agent_ID=t01.Agent_ID

WHERE 		Year IN (2015,2016)
) WITH DATA
PRIMARY INDEX (Client_ID, Year)
ON COMMIT PRESERVE ROWS
;

CREATE VOLATILE TABLE vt_agent_details_2 AS
(
SELECT
	pop.Client_ID
	, CASE WHEN t01.Year IS NOT NULL THEN t01.Year
			WHEN t02.Year IS NOT NULL THEN t02.Year
	END AS Year

	, CASE WHEN t01.Transaction_ID IS NOT NULL THEN t01.Transaction_ID
			WHEN t02.Transaction_ID IS NOT NULL THEN t02.Transaction_ID
	END AS Transaction_ID
	
	, CASE WHEN t01.Agent_ID IS NOT NULL THEN t01.Agent_ID
			WHEN t02.Agent_ID IS NOT NULL THEN t02.Agent_ID
	END AS Agent_ID
	
	, CASE WHEN t01.Agent_Name IS NOT NULL THEN t01.Agent_Name
			WHEN t02.Agent_Name IS NOT NULL THEN t02.Agent_Name
	END AS Agent_Name
	
FROM 		vt_population as pop

LEFT JOIN	
			(SELECT
			CAST(a.Client_ID as DECIMAL(12,0)) AS Client_ID
			, a.Year
			, b.Transaction_ID
			, a.Agent_Name
			, a.Agent_ID
			FROM		DB.Client_Forms as a
			LEFT JOIN 	DB.Form_transactions as b
			ON 		a.Transaction_ID=b.Transaction_ID
			WHERE		Client_ID IN (SELECT Client_ID from vt_population)
			AND 		b.Role_code=50
			AND 		b.Form_type=9000
			AND 		b.Form_status=1
			QUALIFY ROW_NUMBER()OVER(PARTITION BY Client_ID, Year, ORDER BY Form_created_Date DESC)=1
			) t01
ON 			pop.Client_ID=t01.Client_ID

LEFT JOIN	
			(SELECT
			CAST(a.Client_ID as DECIMAL(12,0)) AS Client_ID
			, a.Year
			, b.Transaction_ID
			, a.Agent_Name
			, a.Agent_ID
			FROM		DB.Player_Forms as a
			LEFT JOIN 	DB.Form_transactions as b
			ON 		a.Transaction_ID=b.Transaction_ID
			WHERE		Client_ID IN (SELECT Client_ID from vt_population)
			AND 		b.Role_code=100
			AND 		b.Form_type=9500
			AND 		b.Form_status=1
			QUALIFY ROW_NUMBER()OVER(PARTITION BY Client_ID, Year, ORDER BY Form_created_Date DESC)=1
			) t02
ON 			pop.Client_ID=t02.Client_ID

WHERE		YEAR IN (2015,2016)
)WITH DATA
PRIMARY INDEX (Client_ID,YEAR)
ON COMMIT PRESERVE ROWS
;

CREATE VOLATILE TABLE vt_agent_role_check AS
(
SELECT
	pop.Client_ID
	, 'Star' as Agent_Role 
	, t01.Year
	, Trim(Leading'0' from t01.Agent_ID) as Agent_ID
	, Agent_Name

FROM 		vt_population as pop

INNER JOIN	vt_agent_details_1 AS t01
ON		pop.Client_ID=t01.Client_ID

QUALIFY ROW_NUMBER() OVER(PARTITION BY pop.Client_ID, Agent_Role, t01.Year, Agent_ID ORDER BY Agent_Name DESC)=1

UNION

SELECT
	pop.Client_ID
	, 'Clover' as Agent_Role 
	, t01.Year
	, Trim(Leading'0' from t01.Agent_ID) as Agent_ID
	, Agent_Name

FROM 		vt_population as pop

INNER JOIN	vt_agent_details_2 AS t01
ON		pop.Client_ID=t01.Client_ID

QUALIFY ROW_NUMBER() OVER(PARTITION BY pop.Client_ID, Agent_Role, t01.Year, Agent_ID ORDER BY Agent_Name DESC)=1
) WITH DATA
PRIMARY INDEX
ON COMMIT PRESERVE ROWS
;

SELECT * FROM vt_agent_role_check
order by 1,2
;

CREATE VOLATILE TABLE vt_agent_role_base AS
(
SELECT
	t01.TFN
	, t01.Year
	, t01.Agent_Role
	, t01.Agent_ID
	, t01.Agent_Name

	, CASE WHEN t01.Year=2015 AND t01.Agent_Role='Star' AND t01.Agent_Number IS NOT NULL AND t02.Year=2015 AND t02.Agent_Role='Clover' AND t02.Agent_Number IS NOT NULL AND t01.Agent_Number
=t02.Agent_Number
				THEN 'Y'
			WHEN t01.Year=2015 AND t01.Agent_Role='Clover' AND t01.Agent_Number IS NOT NULL AND t02.Year=2015 AND t02.Agent_Role='Star' AND t02.Agent_Number IS NOT NULL AND t01.Agent_Number
=t02.Agent_Number
				THEN 'Y'
			WHEN t01.Year=2016 AND t01.Agent_Role='Star' AND t01.Agent_Number IS NOT NULL AND t02.Year=2016 AND t02.Agent_Role='Clover' AND t02.Agent_Number IS NOT NULL AND t01.Agent_Number
=t02.Agent_Number
				THEN 'Y'				
			WHEN t01.Year=2016 AND t01.Agent_Role='Clover' AND t01.Agent_Number IS NOT NULL AND t02.Year=2016 AND t02.Agent_Role='Star' AND t02.Agent_Number IS NOT NULL AND t01.Agent_Number
=t02.Agent_Number
				THEN 'Y'
			ELSE 'N'
	END AS Both_roles

	, CASE WHEN t01.Year=2015 AND t01.Agent_Role='Star' AND t01.Agent_Number IS NOT NULL AND t02.Year=2015 AND t02.Agent_Role='Clover' AND t02.Agent_Number IS NOT NULL AND t01.Agent_Number
<>t02.Agent_Number
				THEN 'Y'
			WHEN t01.Year=2015 AND t01.Agent_Role='Clover' AND t01.Agent_Number IS NOT NULL AND t02.Year=2015 AND t02.Agent_Role='Star' AND t02.Agent_Number IS NOT NULL AND t01.Agent_Number
<>t02.Agent_Number
				THEN 'Y'
			WHEN t01.Year=2016 AND t01.Agent_Role='Star' AND t01.Agent_Number IS NOT NULL AND t02.Year=2016 AND t02.Agent_Role='Clover' AND t02.Agent_Number IS NOT NULL AND t01.Agent_Number
<>t02.Agent_Number
				THEN 'Y'				
			WHEN t01.Year=2016 AND t01.Agent_Role='Clover' AND t01.Agent_Number IS NOT NULL AND t02.Year=2016 AND t02.Agent_Role='Star' AND t02.Agent_Number IS NOT NULL AND t01.Agent_Number
<>t02.Agent_Number
				THEN 'Y'
			ELSE 'N'
	END AS Different_agents

	, CASE WHEN t01.Year=2015 AND t01.Agent_Role='Star' AND t01.Agent_Number IS NOT NULL AND t02.Year=2015 AND t02.Agent_Role='Clover' AND t02.Agent_Number IS NULL
				THEN 'Y'
			WHEN t01.Year=2015 AND t01.Agent_Role='Clover' AND t01.Agent_Number IS NOT NULL AND t02.Year=2015 AND t02.Agent_Role='Star' AND t02.Agent_Number IS NULL
				THEN 'Y'
			WHEN t01.Year=2016 AND t01.Agent_Role='Star' AND t01.Agent_Number IS NOT NULL AND t02.Year=2016 AND t02.Agent_Role='Clover' AND t02.Agent_Number IS NULL
				THEN 'Y'				
			WHEN t01.Year=2016 AND t01.Agent_Role='Clover' AND t01.Agent_Number IS NOT NULL AND t02.Year=2016 AND t02.Agent_Role='Star' AND t02.Agent_Number IS NULL
				THEN 'Y'
			ELSE 'N'
	END AS Only_one_role

	, CASE WHEN t01.Year=2015 AND t01.Agent_Role='Star' AND t01.Agent_Number IS NULL AND t02.Year=2015 AND t02.Agent_Role='Clover' AND t02.Agent_Number IS NULL
				THEN 'Y'
			WHEN t01.Year=2015 AND t01.Agent_Role='Clover' AND t01.Agent_Number IS NULL AND t02.Year=2015 AND t02.Agent_Role='Star' AND t02.Agent_Number IS NULL
				THEN 'Y'
			WHEN t01.Year=2016 AND t01.Agent_Role='Star' AND t01.Agent_Number IS NULL AND t02.Year=2016 AND t02.Agent_Role='Clover' AND t02.Agent_Number IS NULL
				THEN 'Y'				
			WHEN t01.Year=2016 AND t01.Agent_Role='Clover' AND t01.Agent_Number IS NULL AND t02.Year=2016 AND t02.Agent_Role='Star' AND t02.Agent_Number IS NULL
				THEN 'Y'
			ELSE 'N'
	END AS No_role

FROM 		vt_agent_role_check as t01

INNER JOIN	vt_agent_role_check as t02
ON 		t01.Client_ID=t02.Client_ID
AND 		t01.Year=t02.Year

QUALIFY ROW_NUMBER() OVER(PARTITION BY t01.Client_ID, t01.Year ORDER BY Both Roles DESC, Different_agents DESC, Only_one_role DESC, No_role DESC)=1					
) WITH DATA
PRIMARY INDEX (Client_ID, Year, Agent_ID)
ON COMMIT PRESERVE ROWS
;

SELECT * FROM vt_agent_role_base
order by 1,2
;

SELECT
	pop.Year
	, t00.Both_role_cnt
	, t01.Diff_role_cnt
	, t02.One_role_cnt
	, t03.No_role_cnt

FROM 	vt_agent_role_base	as pop

LEFT JOIN	
			(SELECT
				Year
				, Count(Agent_Role) as Both_role_cnt

			FROM 	vt_agent_role_base
			WHERE 	Both_roles='Y'
			GROUP BY 1
			) t00
ON 			pop.Year=t00.Year

LEFT JOIN	
			(SELECT
				Year
				, Count(Agent_Role) as Diff_role_cnt

			FROM 	vt_agent_role_base
			WHERE 	Different_agents='Y'
			GROUP BY 1
			) t01
ON 			pop.Year=t01.Year

LEFT JOIN	
			(SELECT
				Year
				, Count(Agent_Role) as One_role_cnt

			FROM 	vt_agent_role_base
			WHERE 	Only_one_role='Y'
			GROUP BY 1
			) t02
ON 			pop.Year=t02.Year

LEFT JOIN	
			(SELECT
				Year
				, Count(Agent_Role) as No_role_cnt

			FROM 	vt_agent_role_base
			WHERE 	No_role='Y'
			GROUP BY 1
			) t03
ON 			pop.Year=t03.Year			

GROUP BY 1,2,3,4,5
ORDER BY 1
;
