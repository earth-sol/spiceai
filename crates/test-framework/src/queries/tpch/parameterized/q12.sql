select
    l_shipmode,
    sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
    sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
from
    lineitem
        join
    orders
    on
            l_orderkey = o_orderkey
where
        l_shipmode in (?, ?)
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date ?
  and l_receiptdate < date ?
group by
    l_shipmode
order by
    l_shipmode;