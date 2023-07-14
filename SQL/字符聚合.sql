select 	customer_guid,
max(wx_name) wx_name ,max(wx_nick) wx_nick, max(head_icon)  head_icon  ,	
concat_ws(',',collect_set(concat('★',coalesce(a.rk,''),wx_name,create_time)))  wx_name_1  ----小课名称
from(
select  row_number()over(partition by lower(customer_guid) order by create_time) as rk ,
customer_guid,	
wx_nick, head_icon,	
wx_name,create_time
from ( 
select  distinct 	
customer_guid,	
wx_nick, head_icon,	
wx_name,create_time
from  dwd_mkt_friend_member_fx
)a 
)a 
group  by  customer_guid