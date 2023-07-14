CREATE TABLE hotel_daily_prepare_{env}
(
  oyo_id            nvarchar2(255)                      not null,
  calc_date         char(10)                           not null,
  base              number,
  two_week_arr number,
  created_at        timestamp default CURRENT_TIMESTAMP not null
);

create index idx_{env}_oyo_id on hotel_daily_prepare_{env}(oyo_id);

create index idx_{env}_calc_date on hotel_daily_prepare_{env}(calc_date);