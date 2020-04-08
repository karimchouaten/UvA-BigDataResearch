# AUTHOR: KARIM CHOUATEN
# RESEARCH: PERFORMANCE DIFFERENCES USING SERIAL AND PARALLEL PROCESSING METHODS IN BIG DATA ANALYTICS AT AFC AJAX FOR THE UNIVERSITY OF AMSTERDAM
# DESCRIPTION: 
# SPARK SQL QUERY - CALCULATING THE LOAD PER PLAYER USING SIX DATASOURCES - RUNTIME IS MEASURED
# CODE WAS EXECUTED USING AZURE DATABRICKS

import time as time

start_time = time.time()

df = spark.sql("""

SELECT Recording_Date AS Date
     ,qa.Exercise_starttime AS Time
	 ,qa.Team_id
	 ,t.TeamName as Activity_Team
	 ,qa.Player_id
     
	 ,CASE
		WHEN qa.Exercise_Name LIKE '%Ajax%' THEN Exercise_Part
		WHEN qa.Exercise_Name LIKE '%w-up%' THEN 'W-up'
		WHEN qa.Exercise_Name LIKE '%extra%' THEN 'Extra'
		WHEN qa.Exercise_Name LIKE '%individueel' THEN 'Individueel'
		WHEN qa.Exercise_Name LIKE '%rehab%' THEN 'Individueel'
		WHEN qa.Exercise_Name LIKE '%) %' THEN RIGHT(qa.Exercise_Name, LENGTH(qa.Exercise_Name) - (INSTR(')', qa.Exercise_Name)))
		ELSE qa.Exercise_Name
        END AS Exercise_Name
        
	 ,CASE
	    WHEN qa.Exercise_Name LIKE '([0-9]%' THEN SUBSTRING(Exercise_Name,2,1)
		ELSE NULL END AS Exercise_nr
        
	 ,CASE INSTR(';',qa.exerciseremark) WHEN 0 THEN NULL
	   ELSE SUBSTRING(qa.exerciseremark, 1, INSTR(';',qa.exerciseremark)-1) END AS Exercise_name_toevoeging
     
     ,CASE 
	   WHEN INSTR(';', INSTR(';',exerciseremark)+1) > 0
       
	   THEN RIGHT(exerciseremark, INSTR(';',REVERSE(exerciseremark))-1)
       
	   ELSE NULL END AS Comment
     
	 , (CASE
		WHEN qa.Exercise_Name LIKE '%Ajax%' THEN qa.Exercise_Name
		ELSE Exercise_Part END) AS Exercise_Part
        
	  ,CASE
		WHEN INSTR(';', INSTR(';',exerciseremark)+1) > 0
		THEN SUBSTRING(exerciseremark, INSTR(';',exerciseremark)+1
			 ,LENGTH(exerciseremark) - INSTR(';',exerciseremark) - LENGTH(RIGHT(exerciseremark, LENGTH(exerciseremark)-INSTR(';',INSTR(';',exerciseremark)+1)+1))
			 )
		ELSE NULL END
		AS Veldgrootte
     
     ,time AS Duration
     
	 ,'LPM' as System

     , (CASE
		WHEN qa.typedataset = 'Match' THEN 'WD'
		WHEN qa.typedataset = 'Test match' THEN 'OEF'
		WHEN (qa.exerciseremark LIKE '%RH%' AND qa.Exercise_name LIKE '%individueel%') THEN 'RH'
		WHEN (qa.exerciseremark LIKE '%IT%' AND qa.Exercise_name LIKE '%individueel%') THEN 'IT'
		WHEN qa.Exercise_name LIKE '%rehab%' THEN 'RH'
		ELSE 'TR' END) as Activity

	 , Player_role AS Positie_wedstrijd
	 , Distance AS Total_Distance
	 , DistanceinSpeed0 AS Distance_0_B_7_kmu
	 , DistanceinSpeed1 AS Distance_7_B_12_kmu
	 , DistanceinSpeed2 AS Distance_12_B_15_kmu
	 , DistanceinSpeed3 AS Distance_15_B_17_DOT_5_kmu
	 , DistanceinSpeed4 AS High_Intensity_Distance_17_DOT_5_B_22_DOT_5_kmu
	 , DistanceinSpeed5 AS Sprint_Distance_BT_22_DOT_5_kmu
	 , (DistanceinSpeed4 + DistanceinSpeed5) AS High_Intensity_Distance_BT_17_DOT_5_kmu
     
	 , (ACCEL_CAT_1 + DECEL_CAT_1) AS Accelerations_Low
	 , (ACCEL_CAT_2 + DECEL_CAT_2) AS Accelerations_Medium
	 , (ACCEL_CAT_3 + DECEL_CAT_3) AS Accelerations_High
	 , ((HR_BIN0_TIME_S  + HR_BIN1_TIME_S + HR_BIN2_TIME_S) / 60) AS Heart_Rate_Low
	 , HR_BIN3_TIME_S / 60 AS Heart_Rate_Medium
	 , ((HR_BIN4_TIME_S  + HR_BIN5_TIME_S) / 60) AS Heart_Rate_High
	 , ((HR_BIN4_TIME_S  + HR_BIN5_TIME_S) / 60) AS Heart_Rate_BT_86
     
     
	 , HR_BIN4_TIME_S / 60 AS Heart_Rate_86_B_93
	 , HR_BIN5_TIME_S / 60 AS Heart_Rate_BT_93
	 ,HR_Max_Exercise AS Maximum_Heart_Rate
	 ,NULL AS Relative_Maximum_Heart_Rate
	 ,Heart_Avarage AS Mean_HR
	 ,(Heart_Avarage / pl.IN_PlayerMaxHeartbeat)*100 AS Relative_Mean_HR
	 , Heart_Index AS HR_Index
	 , (qa.maxSpeed * 3.6) AS Maximum_Velocity
	 , NULL as Dive_Intensity_Left_Low
	 , NULL as Dive_Intensity_Left_Medium
	 , NULL as Dive_Intensity_Left_High
	 , NULL as Dive_Intensity_Right_Low
	 , NULL as Dive_Intensity_Right_Medium
	 , NULL as Dive_Intensity_Right_High
	 , NULL as Jump_Count_Low
	 , NULL as Jump_Count_Medium
	 , NULL as Jump_Count_High
	 , qa.Exclude AS Error_XY
     
     ,(CASE
		WHEN HRstoring = 1 THEN 1
		ELSE qa.ExcludeHR END) as Error_HR
        
	 ,AM_PM
     
	 ,(CASE
	    WHEN qa.Team_id = 1 THEN d.Ajax1_DaysBeforeMatch
		WHEN qa.Team_id = 2 THEN d.JongAjax_DaysBeforematch
		WHEN qa.Team_id = 3 THEN d.AjaxU19_DaysBeforeMatch
		WHEN qa.Team_id = 5 THEN d.AjaxU17_DaysBeforeMatch
		ELSE NULL END) as DaysBeforeMatch
        
	 ,(CASE
	    WHEN qa.Team_id = 1 THEN d.Ajax1_DaysAfterMatch
		WHEN qa.Team_id = 2 THEN d.JongAjax_DaysAftermatch
		WHEN qa.Team_id = 3 THEN d.AjaxU19_DaysAfterMatch
		WHEN qa.Team_id = 5 THEN d.AjaxU17_DaysAfterMatch
		ELSE NULL END) AS DaysAfterMatch
	 
     ,(SELECT AVG(RPE) FROM RPE_new_Scaled r WHERE qa.Recording_Date = r.Trial_date AND qa.AM_PM = r.AM_PM AND qa.Player_id = r.Player_id) AS RPE 
	 ,(SELECT AVG(TotalScore) FROM ReadinessView_Scaled r WHERE qa.Recording_Date = r.Trial_date AND qa.Player_id = r.Player_id) as ReadinessTotalScore

FROM IN_QueryAjax_Scaled qa

INNER JOIN ALG_Player pl ON qa.Player_id = pl.Player_id
INNER JOIN ALG_Team t ON qa.Team_id = t.Team_id
INNER JOIN ALG_Date d ON qa.Recording_Date = d.Date

WHERE (qa.Exclude IS NULL OR qa.Exclude = 0)

UNION

SELECT c.Date
	 , Start_Time AS Time
	 , c.Team_id
	 , t.TeamName as Activity_Team
	 , c.Player_id
	 , (CASE 
		WHEN c.Period_Name LIKE '%Ajax%' THEN c.Remark
		WHEN c.Period_Name LIKE '%Individueel%' THEN 'Individueel'
		WHEN ((c.Soort_training = 'RH' OR c.Soort_training = 'IT') AND c.Period_Name <> 'Extra') THEN 'Individueel'
	    ELSE c.Period_Name END) AS Exercise_Name
	 , NULL AS Exercise_nr
	 , c.Remark2 as Exercise_name_toevoeging
	 , c.Remark3 as Comment
     , (CASE 
		WHEN c.Period_Name LIKE '%Ajax%' THEN c.Period_Name
	    ELSE c.Remark END) AS Exercise_Part
	 , c.Veld_grootte as Veldgrootte
	 , ((((unix_timestamp(start_time, Total_Duration))*1) + 0.0) / 60) as Duration 
	 , 'CAT' as System
     
	 , c.Soort_training as Activity
	 , NULL as Positie_wedstrijd
	 , c.Total_Distance
     
     
	 , c.Velocity_Band_1_Total_Distance as Distance_0_B_7_kmu
	 , c.Velocity_Band_2_Total_Distance as Distance_7_B_12_kmu
	 , c.Velocity_Band_3_Total_Distance as Distance_12_B_15_kmu
	 , c.Velocity_Band_4_Total_Distance as Distance_15_B_17_DOT_5_kmu
	 , c.Velocity_Band_5_Total_Distance as High_Intensity_Distance_17_DOT_5_B_22_DOT_5_kmu
	 , c.Velocity_Band_6_Total_Distance as Sprint_Distance_BT_22_DOT_5_kmu
	 , (Velocity_Band_5_Total_Distance + Velocity_Band_6_Total_Distance) as High_Intensity_Distance_BT_17_DOT_5_kmu
     
	 , NULL as Accelerations_Low
	 , NULL as Accelerations_Medium
	 , NULL as Accelerations_High
     
	 , (((((unix_timestamp(start_time, Heart_Rate_Band_1_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time, Heart_Rate_Band_2_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time, Heart_Rate_Band_3_Total_Duration))*1) + 0.0) / 60)) as Heart_Rate_Low
     
     , (unix_timestamp(Start_time, Heart_Rate_Band_4_Total_Duration)*1 + 0.0) / 60 as Heart_Rate_Medium 
     
     
	 , (((((unix_timestamp(start_time, Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time, Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60)) as Heart_Rate_High
	 , (((((unix_timestamp(start_time, Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time, Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60)) Heart_Rate_BT_86
	 , ((((unix_timestamp(start_time, Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) as Heart_Rate_86_B_93
	 , ((((unix_timestamp(start_time, Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60) as Heart_Rate_BT_93
	 , Maximum_Heart_Rate
	 
     
     , ((SELECT MAX(cf.Maximum_Heart_Rate) FROM CAT_Final_Scaled cf WHERE cf.Player_id = c.Player_id AND cf.Date = c.Date) / p.IN_PlayerMaxHeartbeat)*100 
	   as Relative_Maximum_Heart_Rate
	 , Mean_Heart_Rate as Mean_HR
     
     
	 , ((SELECT AVG(cf.Mean_Heart_Rate) FROM CAT_Final_Scaled cf WHERE cf.Player_id = c.Player_id AND cf.Date = c.Date) / p.IN_PlayerMaxHeartbeat)*100 
	   as Relative_Mean_HR
       
	 , NULL as HR_Index --c.Heart_Rate_Load_(Average) as HR_Index
	 , c.Maximum_Velocity
     
	 , IMA_Dive_Intensity_Band_1_Left_Count as Dive_Intensity_Left_Low
	 
     , IMA_Dive_Intensity_Band_2_Left_Count as Dive_Intensity_Left_Medium
	 , IMA_Dive_Intensity_Band_3_Left_Count as Dive_Intensity_Left_High
	 , IMA_Dive_Intensity_Band_1_Right_Count as Dive_Intensity_Right_Low
	 , IMA_Dive_Intensity_Band_2_Right_Count as Dive_Intensity_Right_Medium
	 , IMA_Dive_Intensity_Band_3_Right_Count as Dive_Intensity_Right_High
	 , IMA_Jump_Count_Low_Band as Jump_Count_Low
	 , IMA_Jump_Count_Med_Band as Jump_Count_Medium
	 , IMA_Jump_Count_High_Band as Jump_Count_High
	 , c.Exclude as Error_XY
	 , c.ExcludeHR as Error_HR				
	 , c.AM_PM
     
	 , (CASE
	    WHEN c.Team_id = 1 THEN d.Ajax1_DaysBeforeMatch
		WHEN c.Team_id = 2 THEN d.JongAjax_DaysBeforematch
		WHEN c.Team_id = 3 THEN d.AjaxU19_DaysBeforeMatch
		WHEN c.Team_id = 5 THEN d.AjaxU17_DaysBeforeMatch
		ELSE NULL END) as DaysBeforeMatch
        
	 , (CASE
	    WHEN c.Team_id = 1 THEN d.Ajax1_DaysAfterMatch
		WHEN c.Team_id = 2 THEN d.JongAjax_DaysAftermatch
		WHEN c.Team_id = 3 THEN d.AjaxU19_DaysAfterMatch
		WHEN c.Team_id = 5 THEN d.AjaxU17_DaysAfterMatch
		ELSE NULL END) as DaysAfterMatch
        
	 , (SELECT AVG(RPE) FROM RPE_new_Scaled r WHERE c.Date = r.Trial_date AND c.AM_PM = r.AM_PM AND c.Player_id = r.Player_id) as RPE
     
	 , (SELECT AVG(TotalScore) FROM ReadinessView_Scaled r WHERE c.Date = r.Trial_date AND c.Player_id = r.Player_id) as ReadinessTotalScore
     
     
FROM cat_final_Scaled c

INNER JOIN ALG_Player p ON c.Player_id = p.Player_id
INNER JOIN ALG_Team t ON c.Team_id = t.Team_id
INNER JOIN ALG_Date d ON c.Date = d.Date

WHERE (c.Exclude IS NULL OR c.Exclude = 0 OR c.Exclude = 2)

UNION 

SELECT c.Date
	,Start_time AS Time
	, c.Team_id
	, t.TeamName as Activity_Team
	, c.Player_id
	, 'Whole Dataset' as Exercise_Name
	, NULL as Exercise_nr
	, NULL as Exercise_name_toevoeging
	, NULL as Comment
	, 'Whole exercise' as Exercise_Part
	, NULL as Veldgrootte


	,SUM((((unix_timestamp(start_time, Total_Duration))*1) + 0.0) / 60) as Duration
	, 'CAT' as System 
	, c.Soort_training as Activity
	, NULL as Positie_wedstrijd
	, SUM(Total_Distance) as Total_Distance
	, SUM(Velocity_Band_1_Total_Distance) as Distance_0_B_7_kmu
	, SUM(Velocity_Band_2_Total_Distance) as Distance_7_B_12_kmu
	, SUM(Velocity_Band_3_Total_Distance) as Distance_12_B_15_kmu
	, SUM(Velocity_Band_4_Total_Distance) as Distance_15_B_17_DOT_5_kmu
	, SUM(Velocity_Band_5_Total_Distance) as High_Intensity_Distance_17_DOT_5_B_22_DOT_5_kmu
	, SUM(Velocity_Band_6_Total_Distance) as Sprint_Distance_BT_22_DOT_5_kmu 
	, SUM(Velocity_Band_5_Total_Distance + Velocity_Band_6_Total_Distance) as High_Intensity_Distance_BT_17_DOT_5_kmu
	, NULL as Accelerations_Low		
	, NULL as Accelerations_Medium	
	, NULL as Accelerations_High	
	
	, SUM(((((unix_timestamp(start_time, Heart_Rate_Band_1_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time, Heart_Rate_Band_2_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time, Heart_Rate_Band_3_Total_Duration))*1) + 0.0) / 60)) as Heart_Rate_Low
	,SUM((((unix_timestamp(start_time, Heart_Rate_Band_4_Total_Duration))*1) + 0.0) / 60) as Heart_Rate_Medium 
	,SUM(((((unix_timestamp(start_time, Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time, Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60)) as Heart_Rate_High
	, SUM(((((unix_timestamp(start_time, Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time, Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60)) Heart_Rate_BT_86
	, SUM((((unix_timestamp(start_time, Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) as Heart_Rate_86_B_93
	, SUM((((unix_timestamp(start_time, Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60) as Heart_Rate_BT_93

	, MAX(Maximum_Heart_Rate) as Maximum_Heart_Rate
	, MAX(Maximum_Heart_Rate) / p.IN_PlayerMaxHeartbeat*100 as Relative_Maximum_Heart_Rate
	, MEAN(Mean_Heart_Rate) as Mean_HR
	, MEAN(Maximum_Heart_Rate) / p.IN_PlayerMaxHeartbeat*100 as Relative_Mean_HR
    
	, NULL AS HR_Index --sum(Heart Rate Load (Average)) as HR_Index
    
	,MAX(Maximum_Velocity) as Maximum_Velocity
	,SUM(IMA_Dive_Intensity_Band_1_Left_Count) as Dive_Intensity_Left_Low
	,SUM(IMA_Dive_Intensity_Band_2_Left_Count) as Dive_Intensity_Left_Medium
	,SUM(IMA_Dive_Intensity_Band_3_Left_Count) as Dive_Intensity_Left_High
	,SUM(IMA_Dive_Intensity_Band_1_Right_Count) as Dive_Intensity_Right_Low
	,SUM(IMA_Dive_Intensity_Band_2_Right_Count) as Dive_Intensity_Right_Medium
	,SUM(IMA_Dive_Intensity_Band_3_Right_Count) as Dive_Intensity_Right_High
	,SUM(IMA_Jump_Count_Low_Band) as Jump_Count_Low
	,SUM(IMA_Jump_Count_Med_Band) as Jump_Count_Medium
	,SUM(IMA_Jump_Count_High_Band) as Jump_Count_High
	,MAX(Exclude) as Error_XY
	,MAX(ExcludeHR) as Error_HR			
	, c.AM_PM

	, (CASE
	WHEN c.Team_id = 1 THEN d.Ajax1_DaysBeforeMatch
		WHEN c.Team_id = 2 THEN d.JongAjax_DaysBeforematch
		WHEN c.Team_id = 3 THEN d.AjaxU19_DaysBeforeMatch
		WHEN c.Team_id = 5 THEN d.AjaxU17_DaysBeforeMatch
		ELSE NULL END) as DaysBeforeMatch

	,(CASE
	WHEN c.Team_id = 1 THEN d.Ajax1_DaysAfterMatch
		WHEN c.Team_id = 2 THEN d.JongAjax_DaysAftermatch
		WHEN c.Team_id = 3 THEN d.AjaxU19_DaysAfterMatch
		WHEN c.Team_id = 5 THEN d.AjaxU17_DaysAfterMatch
		ELSE NULL END) as DaysAfterMatch

	 --,(SELECT AVG(RPE) FROM RPE_new_Scaled r WHERE c.Date = r.Trial_date AND c.AM_PM = r.AM_PM AND c.Player_id = r.Player_id) as RPE 
	 --,(SELECT AVG(TotalScore) FROM ReadinessView_Scaled r WHERE c.Date = r.Trial_date AND c.Player_id = r.Player_id) as ReadinessTotalScore
     
     , NULL AS RPE
     , NULL AS ReadinessTotalScore

FROM CAT_Final_Scaled c
INNER JOIN ALG_Player p ON c.Player_id = p.Player_id
INNER JOIN ALG_Team t ON c.Team_id = t.Team_id
INNER JOIN ALG_Date d ON c.Date = d.Date

WHERE c.Period_Name <> 'Extra'
AND c.Period_Name <> 'individueel'
AND c.Period_Name <> 'herstel'
AND c.Soort_training = 'TR'
AND (c.Exclude IS NULL OR c.Exclude = 0)

GROUP BY c.Date
     , Start_time
	 , c.AM_PM
	 , c.Player_id
	 , c.Soort_training
	 , (p.Initial + '. ' + p.LastName)
	 , c.Team_id
	 , t.TeamName
	 , p.IN_PlayerMaxHeartbeat
	 , c.Exclude
	 , Ajax1_DaysBeforeMatch, JongAjax_DaysBeforematch, AjaxU19_DaysBeforeMatch, AjaxU17_DaysBeforeMatch, Ajax1_DaysAfterMatch, JongAjax_DaysAftermatch, AjaxU19_DaysAfterMatch, AjaxU17_DaysAfterMatch
     
     
UNION

SELECT c.Date
	 --, Start_time AS Time
	 , NULL AS Time
	 , c.Team_id
	 , t.TeamName AS Activity_Team
	 , c.Player_id
	 , 'Whole Match' AS Exercise_Name
	 , NULL AS Exercise_nr
	 , NULL AS Exercise_name_toevoeging
	 , NULL AS Comment

	 , c.Period_Name AS Exercise_Part
	 , NULL AS Veldgrootte
	 , SUM((((unix_timestamp(start_time,Total_Duration))*1) + 0.0) / 60) AS Duration 
	 , 'CAT' AS System 

	 , c.Soort_training AS Activity
	 , NULL AS Positie_wedstrijd
	 , SUM(Total_Distance) AS Total_Distance
	 , SUM(Velocity_Band_1_Total_Distance) AS Distance_0_B_7_kmu
	 , SUM(Velocity_Band_2_Total_Distance) AS Distance_7_B_12_kmu
	 , SUM(Velocity_Band_3_Total_Distance) AS Distance_12_B_15_kmu
	 , SUM(Velocity_Band_4_Total_Distance) AS Distance_15_B_17_DOT_5_kmu
	 , SUM(Velocity_Band_5_Total_Distance) AS High_Intensity_Distance_17_DOT_5_B_22_DOT_5_kmu
	 , SUM(Velocity_Band_6_Total_Distance) AS Sprint_Distance_BT_22_DOT_5_kmu
	 , SUM(Velocity_Band_5_Total_Distance + Velocity_Band_6_Total_Distance) AS High_Intensity_Distance_BT_17_DOT_5_kmu
	 , NULL AS Accelerations_Low
	 , NULL AS Accelerations_Medium
	 , NULL AS Accelerations_High
	 , SUM(((((unix_timestamp(start_time,Heart_Rate_Band_1_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_2_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_3_Total_Duration))*1) + 0.0) / 60)) AS Heart_Rate_Low

	 , SUM((((unix_timestamp(start_time,Heart_Rate_Band_4_Total_Duration))*1) + 0.0) / 60) AS Heart_Rate_Medium

	 , SUM(((((unix_timestamp(start_time,Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60)) AS Heart_Rate_High

	 , SUM(((((unix_timestamp(start_time,Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60)) AS Heart_Rate_BT_86
	 , SUM((((unix_timestamp(start_time,Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) AS Heart_Rate_86_B_93
	 , SUM((((unix_timestamp(start_time,Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60) AS Heart_Rate_BT_93
	 , MAX(Maximum_Heart_Rate) AS Maximum_Heart_Rate
	 , MAX(Maximum_Heart_Rate) / p.IN_PlayerMaxHeartbeat*100 AS Relative_Maximum_Heart_Rate
	 , AVG(Mean_Heart_Rate) AS Mean_HR


	 , AVG(Maximum_Heart_Rate) / p.IN_PlayerMaxHeartbeat*100 AS Relative_Mean_HR
	 , NULL AS HR_Index --sum(Heart_Rate_Load (Average)) AS HR_Index
	 , MAX(Maximum_Velocity) AS Maximum_Velocity
	 , SUM(IMA_Dive_Intensity_Band_1_Left_Count) AS Dive_Intensity_Left_Low
	 , SUM(IMA_Dive_Intensity_Band_2_Left_Count) AS Dive_Intensity_Left_Medium
	 , SUM(IMA_Dive_Intensity_Band_3_Left_Count) AS Dive_Intensity_Left_High
	 , SUM(IMA_Dive_Intensity_Band_1_Right_Count) AS Dive_Intensity_Right_Low
	 , SUM(IMA_Dive_Intensity_Band_2_Right_Count) AS Dive_Intensity_Right_Medium
	 , SUM(IMA_Dive_Intensity_Band_3_Right_Count) AS Dive_Intensity_Right_High
	 , SUM(IMA_Jump_Count_Low_Band) AS Jump_Count_Low
	 , SUM(IMA_Jump_Count_Med_Band) AS Jump_Count_Medium
	 , SUM(IMA_Jump_Count_High_Band) AS Jump_Count_High
	 , MAX(Exclude) AS Error_XY
	 , MAX(ExcludeHR) AS Error_HR					
	 , c.AM_PM
	 , (CASE
	    WHEN c.Team_id = 1 THEN d.Ajax1_DaysBeforeMatch
		WHEN c.Team_id = 2 THEN d.JongAjax_DaysBeforematch
		WHEN c.Team_id = 3 THEN d.AjaxU19_DaysBeforeMatch
		WHEN c.Team_id = 5 THEN d.AjaxU17_DaysBeforeMatch
		ELSE NULL END) AS DaysBeforeMatch
	 , (CASE
	    WHEN c.Team_id = 1 THEN d.Ajax1_DaysAfterMatch
		WHEN c.Team_id = 2 THEN d.JongAjax_DaysAftermatch
		WHEN c.Team_id = 3 THEN d.AjaxU19_DaysAfterMatch
		WHEN c.Team_id = 5 THEN d.AjaxU17_DaysAfterMatch
		ELSE NULL END) AS DaysAfterMatch
     
     , NULL AS RPE
     , NULL AS ReadinessTotalScore

FROM CAT_Final_Scaled c
INNER JOIN ALG_Player p ON c.Player_id = p.Player_id
INNER JOIN ALG_Team t ON c.Team_id = t.Team_id
INNER JOIN ALG_Date d ON c.Date = d.Date

WHERE c.Period_Name LIKE '%Ajax%'
AND (Soort_training = 'WD')
AND (c.Exclude IS NULL OR c.Exclude = 0)

GROUP BY c.Date
	 , c.AM_PM
	 , c.Player_id
	 , c.Period_Name
	 , Soort_training
	 , (p.Initial + '. ' + p.LastName)
	 , c.Team_id
	 , t.TeamName
	 , p.IN_PlayerMaxHeartbeat
	 , c.Exclude
	 , Ajax1_DaysBeforeMatch, JongAjax_DaysBeforematch, AjaxU19_DaysBeforeMatch, AjaxU17_DaysBeforeMatch, Ajax1_DaysAfterMatch, JongAjax_DaysAftermatch, AjaxU19_DaysAfterMatch, AjaxU17_DaysAfterMatch

UNION

SELECT c.Date
	 --, Start_time AS Time
	 , NULL AS Time
	 , c.Team_id
	 , t.TeamName AS Activity_Team
	 , c.Player_id
	 , 'Whole Game' AS Exercise_Name
	 , NULL AS Exercise_nr
	 , NULL AS Exercise_name_toevoeging
	 , NULL AS Comment
	 , c.Period_Name AS Exercise_Part
	 , NULL AS Veldgrootte
	 , SUM((((unix_timestamp(start_time,Total_Duration))*1) + 0.0) / 60) AS Duration 
	 , 'CAT' AS System 
	 , c.Soort_training AS Activity
	 , NULL AS Positie_wedstrijd
	 , SUM(Total_Distance) AS Total_Distance
	 , SUM(Velocity_Band_1_Total_Distance) AS Distance_0_B_7_kmu
	 , SUM(Velocity_Band_2_Total_Distance) AS Distance_7_B_12_kmu
	 , SUM(Velocity_Band_3_Total_Distance) AS Distance_12_B_15_kmu
	 , SUM(Velocity_Band_4_Total_Distance) AS Distance_15_B_17_DOT_5_kmu
	 , SUM(Velocity_Band_5_Total_Distance) AS High_Intensity_Distance_17_DOT_5_B_22_DOT_5_kmu
	 , SUM(Velocity_Band_6_Total_Distance) AS Sprint_Distance_BT_22_DOT_5_kmu
	 , SUM(Velocity_Band_5_Total_Distance + Velocity_Band_6_Total_Distance) AS High_Intensity_Distance_BT_17_DOT_5_kmu
	 , NULL AS Accelerations_Low	
	 , NULL AS Accelerations_Medium	
	 , NULL AS Accelerations_High
	 
	 , SUM(((((unix_timestamp(start_time,Heart_Rate_Band_1_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_2_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_3_Total_Duration))*1) + 0.0) / 60)) AS Heart_Rate_Low
	 , SUM((((unix_timestamp(start_time,Heart_Rate_Band_4_Total_Duration))*1) + 0.0) / 60) AS Heart_Rate_Medium
	 , SUM(((((unix_timestamp(start_time,Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60)) AS Heart_Rate_High
		
	 , SUM(((((unix_timestamp(start_time,Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60)) AS Heart_Rate_BT_86
	 , SUM((((unix_timestamp(start_time,Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) AS Heart_Rate_86_B_93
	 , SUM((((unix_timestamp(start_time,Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60) AS Heart_Rate_BT_93
	 , MAX(Maximum_Heart_Rate) AS Maximum_Heart_Rate
	 , MAX(Maximum_Heart_Rate) / p.IN_PlayerMaxHeartbeat*100 AS Relative_Maximum_Heart_Rate
	 , AVG(Mean_Heart_Rate) AS Mean_HR
	 , AVG(Maximum_Heart_Rate) / p.IN_PlayerMaxHeartbeat*100 AS Relative_Mean_HR
	 , NULL AS HR_Index--, sum(Heart_Rate_Load (Average)) AS HR_Index
	 , MAX(Maximum_Velocity) AS Maximum_Velocity
	 , SUM(IMA_Dive_Intensity_Band_1_Left_Count) AS Dive_Intensity_Left_Low
	 , SUM(IMA_Dive_Intensity_Band_2_Left_Count) AS Dive_Intensity_Left_Medium
	 , SUM(IMA_Dive_Intensity_Band_3_Left_Count) AS Dive_Intensity_Left_High
	 , SUM(IMA_Dive_Intensity_Band_1_Right_Count) AS Dive_Intensity_Right_Low
	 , SUM(IMA_Dive_Intensity_Band_2_Right_Count) AS Dive_Intensity_Right_Medium
	 , SUM(IMA_Dive_Intensity_Band_3_Right_Count) AS Dive_Intensity_Right_High
	 , SUM(IMA_Jump_Count_Low_Band) AS Jump_Count_Low
	 , SUM(IMA_Jump_Count_Med_Band) AS Jump_Count_Medium
	 , SUM(IMA_Jump_Count_High_Band) AS Jump_Count_High
	 , MAX(Exclude) AS Error_XY
	 , MAX(ExcludeHR) AS Error_HR					
	 , c.AM_PM
	 , (CASE
	    WHEN c.Team_id = 1 THEN d.Ajax1_DaysBeforeMatch
		WHEN c.Team_id = 2 THEN d.JongAjax_DaysBeforematch
		WHEN c.Team_id = 3 THEN d.AjaxU19_DaysBeforeMatch
		WHEN c.Team_id = 5 THEN d.AjaxU17_DaysBeforeMatch
		ELSE NULL END) AS DaysBeforeMatch

	 , (CASE
	    WHEN c.Team_id = 1 THEN d.Ajax1_DaysAfterMatch
		WHEN c.Team_id = 2 THEN d.JongAjax_DaysAftermatch
		WHEN c.Team_id = 3 THEN d.AjaxU19_DaysAfterMatch
		WHEN c.Team_id = 5 THEN d.AjaxU17_DaysAfterMatch
		ELSE NULL END) AS DaysAfterMatch
     
     , NULL AS RPE
     , NULL AS ReadinessTotalScore

FROM CAT_Final_Scaled c
INNER JOIN ALG_Player p ON c.Player_id = p.Player_id
INNER JOIN ALG_Team t ON c.Team_id = t.Team_id
INNER JOIN ALG_Date d ON c.Date = d.Date

WHERE c.Period_Name LIKE '%Ajax%'
AND Soort_training = 'OEF' 
AND (c.Exclude IS NULL OR c.Exclude = 0)

GROUP BY c.Date
	 , c.AM_PM
	 , c.Player_id
	 , c.Period_Name
	 , Soort_training
	 , (p.Initial + '. ' + p.LastName)
	 , c.Team_id
	 , t.TeamName
	 , p.IN_PlayerMaxHeartbeat
	 , c.Exclude
	 , Ajax1_DaysBeforeMatch, JongAjax_DaysBeforematch, AjaxU19_DaysBeforeMatch, AjaxU17_DaysBeforeMatch, Ajax1_DaysAfterMatch, JongAjax_DaysAftermatch, AjaxU19_DaysAfterMatch, AjaxU17_DaysAfterMatch


UNION

SELECT c.Date
	 --, Start_time AS Time
	 , NULL AS Time
	 , c.Team_id
	 , t.TeamName AS Activity_Team
	 , c.Player_id
	 , c.Period_Name AS Exercise_Name
	 , NULL AS Exercise_nr 
	 , c.Remark2 AS Exercise_name_toevoeging
	 , NULL AS Comment
	 , 'Whole exercise' AS Exercise_Part
     
	 ,c.Veld_grootte AS Veldgrootte
     
	 , SUM((((unix_timestamp(start_time,Total_Duration))*1) + 0.0) / 60) AS Duration

	 , 'CAT' AS System
	 , c.Soort_training AS Activity
	 , NULL AS Positie_wedstrijd	
	 , SUM(Total_Distance) AS Total_Distance
	 , SUM(Velocity_Band_1_Total_Distance) AS Distance_0_B_7_kmu
	 , SUM(Velocity_Band_2_Total_Distance) AS Distance_7_B_12_kmu
	 , SUM(Velocity_Band_3_Total_Distance) AS Distance_12_B_15_kmu
	 , SUM(Velocity_Band_4_Total_Distance) AS Distance_15_B_17_DOT_5_kmu
	 , SUM(Velocity_Band_5_Total_Distance) AS High_Intensity_Distance_17_DOT_5_B_22_DOT_5_kmu
	 , SUM(Velocity_Band_6_Total_Distance) AS Sprint_Distance_BT_22_DOT_5_kmu
	 , SUM(Velocity_Band_5_Total_Distance + Velocity_Band_6_Total_Distance) AS High_Intensity_Distance_BT_17_DOT_5_kmu 
	 , NULL AS Accelerations_Low
	 , NULL AS Accelerations_Medium
	 , NULL AS Accelerations_High
	 , SUM(((((unix_timestamp(start_time,Heart_Rate_Band_1_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_2_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_3_Total_Duration))*1) + 0.0) / 60)) AS Heart_Rate_Low
	 
	 , SUM((((unix_timestamp(start_time,Heart_Rate_Band_4_Total_Duration))*1) + 0.0) / 60) AS Heart_Rate_Medium

	 , SUM(((((unix_timestamp(start_time,Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60)) AS Heart_Rate_High

	 , SUM(((((unix_timestamp(start_time,Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) + ((((unix_timestamp(start_time,Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60)) AS Heart_Rate_BT_86
	 , SUM((((unix_timestamp(start_time,Heart_Rate_Band_5_Total_Duration))*1) + 0.0) / 60) AS Heart_Rate_86_B_93
	 , SUM((((unix_timestamp(start_time,Heart_Rate_Band_6_Total_Duration))*1) + 0.0) / 60) AS Heart_Rate_BT_93
	 , MAX(Maximum_Heart_Rate) AS Maximum_Heart_Rate
	 , MAX(Maximum_Heart_Rate) / p.IN_PlayerMaxHeartbeat*100 AS Relative_Maximum_Heart_Rate
	 , AVG(Mean_Heart_Rate) AS Mean_HR
	 , AVG(Maximum_Heart_Rate) / p.IN_PlayerMaxHeartbeat*100 AS Relative_Mean_HR
	 , NULL AS HR_Index --SUM(Heart_Rate_Load (Average)) AS HR_Index
	 , MAX(Maximum_Velocity) AS Maximum_Velocity
	 , SUM(IMA_Dive_Intensity_Band_1_Left_Count) AS Dive_Intensity_Left_Low
	 , SUM(IMA_Dive_Intensity_Band_2_Left_Count) AS Dive_Intensity_Left_Medium
	 , SUM(IMA_Dive_Intensity_Band_3_Left_Count) AS Dive_Intensity_Left_High
	 , SUM(IMA_Dive_Intensity_Band_1_Right_Count) AS Dive_Intensity_Right_Low
	 , SUM(IMA_Dive_Intensity_Band_2_Right_Count) AS Dive_Intensity_Right_Medium
	 , SUM(IMA_Dive_Intensity_Band_3_Right_Count) AS Dive_Intensity_Right_High
	 , SUM(IMA_Jump_Count_Low_Band) AS Jump_Count_Low
	 , SUM(IMA_Jump_Count_Med_Band) AS Jump_Count_Medium
	 , SUM(IMA_Jump_Count_High_Band) AS Jump_Count_High
	 , MAX(Exclude) AS Error_XY		--, NULL AS Error_XY
	 , MAX(ExcludeHR) AS Error_HR	--, NULL AS Error_HR						
	 , c.AM_PM

	 , (CASE
	    WHEN c.Team_id = 1 THEN d.Ajax1_DaysBeforeMatch
		WHEN c.Team_id = 2 THEN d.JongAjax_DaysBeforematch
		WHEN c.Team_id = 3 THEN d.AjaxU19_DaysBeforeMatch
		WHEN c.Team_id = 5 THEN d.AjaxU17_DaysBeforeMatch
		ELSE NULL END) AS DaysBeforeMatch

	 , (CASE
	    WHEN c.Team_id = 1 THEN d.Ajax1_DaysAfterMatch
		WHEN c.Team_id = 2 THEN d.JongAjax_DaysAftermatch
		WHEN c.Team_id = 3 THEN d.AjaxU19_DaysAfterMatch
		WHEN c.Team_id = 5 THEN d.AjaxU17_DaysAfterMatch
		ELSE NULL END) AS DaysAfterMatch
     
     , NULL AS RPE
     , NULL AS ReadinessTotalScore

FROM CAT_Final_Scaled c
INNER JOIN ALG_Player p ON c.Player_id = p.Player_id
INNER JOIN ALG_Team t ON c.Team_id = t.Team_id
INNER JOIN ALG_Date d ON c.Date = d.Date

WHERE c.Soort_training = 'TR'
AND (c.Exclude IS NULL OR c.Exclude = 0)
AND c.Remark <> 'Whole exercise'

GROUP BY c.Date
	 , c.AM_PM
	 , c.Player_id
	 , c.Period_Name
	 , c.Remark2
	 , c.Veld_grootte
	 , Soort_training
	 , (p.Initial + '. ' + p.LastName)
	 , c.Team_id
	 , t.TeamName
	 , p.IN_PlayerMaxHeartbeat
	 , Ajax1_DaysBeforeMatch, JongAjax_DaysBeforematch, AjaxU19_DaysBeforeMatch, AjaxU17_DaysBeforeMatch, Ajax1_DaysAfterMatch, JongAjax_DaysAftermatch, AjaxU19_DaysAfterMatch, AjaxU17_DaysAfterMatch

UNION

SELECT l.Date, Time, Team_id, Activity_Team
	 ,Player_id
	 ,"Exercise Name" AS Exercise_Name
	 ,Exercise_nr
	 ,Exercise_name_toevoeging
     ,Comment
	 ,"Exercise Part" AS Exercise_Part
	 ,Veldgrootte
     ,Duration
	 ,System
	 ,Activity
	 ,Positie_wedstrijd
     
     ,"Total Distance" AS Total_Distance
     ,"Distance 0 - 7 km/u" AS Distance_0_B_7_kmu
     ,"Distance 7 - 12 km/u" AS Distance_7_B_12_kmu
     ,"Distance 12 - 15 km/u" AS Distance_12_B_15_kmu
     ,"Distance 15 - 17.5 km/u" AS Distance_15_B_17_DOT_5_kmu
     
	 ,"High Intensity Distance 17.5 - 22.5 km/u" AS High_Intensity_Distance_17_DOT_5_B_22_DOT_5_kmu
     ,"Sprint Distance > 22.5 km/u" AS Sprint_Distance_BT_22_DOT_5_kmu
	 ,"High Intensity Distance > 17.5 km/u" AS High_Intensity_Distance_BT_17_DOT_5_kmu
     
	 ,"Accelerations Low" AS Accelerations_Low
     ,"Accelerations Medium" AS Accelerations_Medium
     ,"Accelerations High" AS Accelerations_High
     
	 ,"Heart Rate Low" AS Heart_Rate_Low
     , "Heart Rate Medium" AS Heart_Rate_Medium
     , "Heart Rate High" AS Heart_Rate_High
     
     , "Heart Rate > 86%" AS Heart_Rate_BT_86
     , "Heart Rate 86% - 93%" AS Heart_Rate_86_B_93
     ,"Heart Rate > 93%" AS Heart_Rate_BT_93
     
	 ,"Maximum Heart Rate" AS Maximum_Heart_Rate
     , "Relative Maximum Heart Rate" AS Relative_Maximum_Heart_Rate
     , "Mean HR" AS Mean_HR
     , "Relative Mean HR" AS Relative_Mean_HR
     , "HR_Index" AS HR_Index
	 ,"Maximum Velocity" AS Maximum_Velocity
     
	 ,NULL AS Dive_Intensity_Left_Low
	 ,NULL AS Dive_Intensity_Left_Medium
	 ,NULL AS Dive_Intensity_Left_High
     
	 ,NULL AS Dive_Intensity_Right_Low
	 ,NULL AS Dive_Intensity_Right_Medium
	 ,NULL AS Dive_Intensity_Right_High
     
	 ,NULL AS Jump_Count_Low
	 ,NULL AS Jump_Count_Medium
	 ,NULL AS Jump_Count_High
	 ,Error_XY
     ,Error_HR
     ,AM_PM
     , (CASE
	    WHEN l.Team_id = 1 THEN d.Ajax1_DaysBeforeMatch
		WHEN l.Team_id = 2 THEN d.JongAjax_DaysBeforematch
		WHEN l.Team_id = 3 THEN d.AjaxU19_DaysBeforeMatch
		WHEN l.Team_id = 5 THEN d.AjaxU17_DaysBeforeMatch
		ELSE NULL END) as DaysBeforeMatch
        
	 , (CASE
	    WHEN l.Team_id = 1 THEN d.Ajax1_DaysAfterMatch
		WHEN l.Team_id = 2 THEN d.JongAjax_DaysAftermatch
		WHEN l.Team_id = 3 THEN d.AjaxU19_DaysAfterMatch
		WHEN l.Team_id = 5 THEN d.AjaxU17_DaysAfterMatch
		ELSE NULL END) as DaysAfterMatch
        
	 , NULL as RPE
	 , NULL as ReadinessTotalScore
     
FROM load_extern_Scaled l

INNER JOIN ALG_Date d ON l.Date = d.Date
""").show()

print("--- %s seconds ---" % (time.time() - start_time))