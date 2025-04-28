select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
                p_partkey = l_partkey
            and p_brand = ?
            and p_container in (?, ?, ?, ?)
            and l_quantity >= ? and l_quantity <= ? + ?
            and p_size between ? and ?
            and l_shipmode in (?, ?)
            and l_shipinstruct = ?
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = ?
            and p_container in (?, ?, ?, ?)
            and l_quantity >= ? and l_quantity <= ? + ?
            and p_size between ? and ?
            and l_shipmode in (?, ?)
            and l_shipinstruct = ?
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = ?
            and p_container in (?, ?, ?, ?)
            and l_quantity >= ? and l_quantity <= ? + ?
            and p_size between ? and ?
            and l_shipmode in (?, ?)
            and l_shipinstruct = ?
        );