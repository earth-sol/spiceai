select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
                p_partkey = l_partkey
            and p_brand = $1
            and p_container in ($2, $3, $4, $5, $6)
            and l_quantity >= $7 and l_quantity <= $8 + $9
            and p_size between $10 and $11
            and l_shipmode in ($12, $13)
            and l_shipinstruct = $14
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = $15
            and p_container in ($16, $17, $18, $19, $20)
            and l_quantity >= $21 and l_quantity <= $22 + $23
            and p_size between $24 and $25
            and l_shipmode in ($26, $27)
            and l_shipinstruct = $28
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = $29
            and p_container in ($30, $31, $32, $33, $34)
            and l_quantity >= $35 and l_quantity <= $36 + $37
            and p_size between $38 and $39
            and l_shipmode in ($40, $41)
            and l_shipinstruct = $42
        );