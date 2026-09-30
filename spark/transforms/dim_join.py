"""JOIN ``dim_join`` from ``daily_pos_sales.mp`` (``xfr/dim_join.xfr``).

Implemented as two broadcast left joins; any line that fails to match
product_dim (on upc) *or* store_dim (on store_id) is what Ab Initio emits on
the JOIN's ``unused0`` port, so it goes to the reject output instead.
"""

from __future__ import annotations

from typing import NamedTuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.schemas import POS_SALES

_PRODUCT_HIT = "_product_matched"
_STORE_HIT = "_store_matched"

JOINED_FIELDS = (
    "business_date",
    "store_id",
    "transaction_id",
    "qty",
    "ext_price",
    "discount_amt",
    "loyalty_id",
    "upc",
    "department",
    "category",
    "brand",
    "private_label_flag",
    "banner",
    "region",
)


class JoinResult(NamedTuple):
    joined: DataFrame
    rejects: DataFrame


def join_dimensions(sales: DataFrame, product_dim: DataFrame, store_dim: DataFrame) -> JoinResult:
    product = product_dim.select(
        "upc",
        "department",
        "category",
        "brand",
        "private_label_flag",
        F.lit(True).alias(_PRODUCT_HIT),
    )
    store = store_dim.select("store_id", "banner", "region", F.lit(True).alias(_STORE_HIT))

    df = sales.join(F.broadcast(product), on="upc", how="left").join(
        F.broadcast(store), on="store_id", how="left"
    )
    matched = F.col(_PRODUCT_HIT).isNotNull() & F.col(_STORE_HIT).isNotNull()

    joined = df.filter(matched).select(*JOINED_FIELDS)
    rejects = df.filter(~matched).select(*POS_SALES.field_names)
    return JoinResult(joined=joined, rejects=rejects)
