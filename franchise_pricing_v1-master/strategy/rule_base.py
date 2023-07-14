import time

import pandas as pd
import numpy as np

from common.util.utils import LogUtil, MiscUtil, DFUtil


def get_ebase_data_from_oracle(smart_hotel_oyo_id_list, start_date, oracle_query_manager, end_day_offset):
    oyo_id_in_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', smart_hotel_oyo_id_list, 1000)
    ebase_query_str = """
        select oyo_id, sku_date, ebase_price
        from OYO_DW_PRICING.PRICING_MODEL_EBASE_PRICE
        where booking_date = '{start_date}'
        and booking_offset <= {end_day_offset}
        and {ids}
        and is_delete = 0
    """.format(start_date=start_date, end_day_offset=end_day_offset, ids=oyo_id_in_str)
    ebase_query_df = oracle_query_manager.read_sql(ebase_query_str, 120)
    ebase_query_df = DFUtil.check_duplicate(ebase_query_df, 'ebase_query_df', ['oyo_id', 'sku_date'])
    return ebase_query_df


def get_all_hotels_next_7d_occ_from_adb(smart_hotel_oyo_id_list, start_date, adb_query_manager, oracle_query_manager):
    adb_query_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', smart_hotel_oyo_id_list, 2000)
    srn_query = """
            select oyo_id, sum(srn) as srn
            from (select id as hotel_id, oyo_id
                  from product_hotel
                  where {0}) hotel
                   left join (select hotel_id, 
                   (ifnull(total_count, 0) - ifnull(blocked_count, 0)) +  (ifnull(additional_count, 0) - ifnull(additional_blocked_count, 0)) as srn 
                   from trade_hotel_inventory where is_deleted=0) inventory
            on hotel.hotel_id = inventory.hotel_id group by oyo_id
            """.format(adb_query_str)

    begin = time.time()
    srn_df = adb_query_manager.read_sql(srn_query)
    LogUtil.get_cur_logger().info('get_all_hotels_next_7d_occ_from_adb, srn cost: %0.2fs', time.time() - begin)

    mm_channel_ids_query_sql = """
    select id
    from product_channel
    where is_valid = 1
    and is_deleted = 0
    and channel_name like 'MM%%'
    """

    mm_channel_ids_query_df = adb_query_manager.read_sql(mm_channel_ids_query_sql, 60)

    mm_channel_ids_lst = list(mm_channel_ids_query_df.id.astype(str))

    mm_channel_ids_str = ', '.join(mm_channel_ids_lst)

    begin = time.time()
    brn_query = """
            select oyo_id,
                   calc_date         as "date",
                   hotel_with_dates.hotel_id,
                   sum(case 
                            when calc_date >= checkin
                            and calc_date < checkout
                            and (checkout_time is null or calc_date < checkout_time) then 1 
                       else 0 end
                       ) as brn,
                   sum(case 
                            when calc_date >= checkin
                            and calc_date < checkout
                            and (checkout_time is null or calc_date < checkout_time) 
                            and source in ({2}) then 1
                       else 0 end) as mm_brn,
                   sum(case
                when calc_date >= checkin
                    and calc_date < checkout
                    and (checkout_time is null or calc_date < checkout_time)
                    and ((source in (1, {2}) and nights >= 14)
                            or
                        (source in (10) and order_type = 3)) then 1
                else 0 end) as long_stay_brn
            from  (
               select oyo_id, hotel_id, calc_date
               from (select id as hotel_id, oyo_id
                     from product_hotel
                     where {0}) hotel
                      cross join (select date_format('{1}' + interval indices.idx day, '%%Y-%%m-%%d') as calc_date
                                  from (select 0 as idx
                                        union
                                        select 1
                                        union
                                        select 2
                                        union
                                        select 3
                                        union
                                        select 4
                                        union
                                        select 5
                                        union
                                        select 6
                                        union
                                        select 7
                                        union
                                        select 8
                                       ) as indices) dates) hotel_with_dates
               left join (select hotel_id, status, checkin, checkout, checkout_time, source, nights, order_type
                          from (select hotel_id,
                                       status,
                                       date_format(check_in, '%%Y-%%m-%%d')                  as checkin,
                                       date_format(check_out, '%%Y-%%m-%%d')                 as checkout,
                                       case
                                         when check_out_time is null or
                                              date_format(check_out_time, '%%Y-%%m-%%d') = '1970-01-01' or
                                              date_format(check_out_time, '%%Y-%%m-%%d') = '0000-00-00' then null
                                         else date_format(check_out_time, '%%Y-%%m-%%d') end as checkout_time,
                                       booking_id,
                                       type as order_type
                                from trade_room_reservation
                                where status in (0, 1, 2, 4)
                                  and is_deleted = 0
                                  and hotel_id in (select id as hotel_id
                                                   from product_hotel
                                                   where {0})
                                  and date_format(check_in, '%%Y-%%m-%%d') <= ('{1}' + interval 8 day)
                                  and date_format(check_out, '%%Y-%%m-%%d') > '{1}') r
                                 inner join (select id, is_deleted, source, datediff(date_format(departure_date, '%%Y-%%m-%%d'),
                                  date_format(arrival_date, '%%Y-%%m-%%d')) as nights from trade_booking) b on r.booking_id = b.id
                          where b.is_deleted = 0) order_rooms on hotel_with_dates.hotel_id = order_rooms.hotel_id
        group by oyo_id, hotel_with_dates.hotel_id, calc_date
        order by 1, 2
    """.format(adb_query_str, start_date, mm_channel_ids_str)

    brn_df = adb_query_manager.read_sql(brn_query)
    LogUtil.get_cur_logger().info('get_all_hotels_next_7d_occ_from_adb, brn cost: %0.2fs', time.time() - begin)

    brn_with_srn_df = pd.merge(brn_df, srn_df, on=['oyo_id'], how='left')

    def calc_occ(brn, long_stay_brn, srn):
        remain_srn = srn - long_stay_brn
        if remain_srn <= 0:
            return 1
        remain_brn = brn - long_stay_brn
        if remain_brn <= 0:
            return 0
        occ = remain_brn / remain_srn
        return 1 if occ > 1 else occ

    # subtract long stay brn for both brn and srn
    brn_with_srn_df = DFUtil.apply_func_for_df(brn_with_srn_df, 'occ', ['brn', 'long_stay_brn', 'srn'],
                                               lambda values: calc_occ(*values))

    return brn_with_srn_df

