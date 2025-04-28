select
            100.00 * sum(case
                             when p_type like ?
                                 then l_extendedprice * (? - l_discount)
                             else ?
            end) / sum(l_extendedprice * (? - l_discount)) as promo_revenue
from
    lineitem,
    part
where
        l_partkey = p_partkey
  and l_shipdate >= date ?
  and l_shipdate < date ?;