from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import round


def get_daily_revenue(orders_df, order_items_df):
    filtered_df = orders_df.filter('order_status in ("COMPLETE","CLOSED")')

    joined_df = filtered_df.join(
        order_items_df, filtered_df.order_id == order_items_df.order_item_order_id)

    grouped_aggregated_df = joined_df. \
        groupBy('order_date', 'order_item_product_id').agg(
            round(_sum('order_item_subtotal'), 2).alias('revenue'))

    final_df = (grouped_aggregated_df
                .orderBy(grouped_aggregated_df.order_date, grouped_aggregated_df.revenue.desc()))

    return final_df
