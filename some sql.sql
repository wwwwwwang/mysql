val sql = """select count(adspace_id)as cnt,
             sum(case when length(imei) > 3 then 1 else 0 end) as cnt_imei,
             sum(case when length(imei) > 3 then 1 else 0 end)*1.0/count(adspace_id) as rate_imei,
             sum(case when length(aid) > 3 then 1 else 0 end) as cnt_aid,
             sum(case when length(aid) > 3 then 1 else 0 end)*1.0/count(adspace_id) as rate_aid,
             sum(case when length(aaid) > 3 then 1 else 0 end) as cnt_aaid,
             sum(case when length(aaid) > 3 then 1 else 0 end)*1.0/count(adspace_id) as rate_aaid,
             sum(case when length(idfa) > 3 then 1 else 0 end) as cnt_idfa,
             sum(case when length(idfa) > 3 then 1 else 0 end)*1.0/count(adspace_id) as rate_idfa
             from andid
             where os like '%0'
             """
			 
	
val res = sqlContext.sql(s"""
select count(time) as total_cnt,
sum(case when time <=1 then 1 else 0 end) as cnt_1,
sum(case when time <=5 then 1 else 0 end) as cnt_5,
sum(case when time <=30 then 1 else 0 end) as cnt_30,
sum(case when time>30 and time <=50 then 1 else 0 end) as cnt_50,
sum(case when time>50 and time <=100 then 1 else 0 end) as cnt_100,
sum(case when time>100 and time <=200 then 1 else 0 end) as cnt_200,
sum(case when time>200 and time <=500 then 1 else 0 end) as cnt_500,
sum(case when time>500 then 1 else 0 end) as cnt_out
from timetable
""")