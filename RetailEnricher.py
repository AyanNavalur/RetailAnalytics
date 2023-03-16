from DirConstants import INPUT_BASE_DIR, OUTPUT_BASE_DIR
from SinkConstants import SINK_FILE_FORMAT, SINK_OUTPUT_MODE, SINK_OUTPUT_PROCESSED_FOLDER_NAME
from SourceConstants import SOURCE_FILE_FORMAT, ORDERS_DATA_PATH, ORDER_ITEMS_DATA_PATH
from RetailProcessor import get_daily_revenue
from Reader import read_from_source
import OrdersSchema
import OrderItemsSchema
from SparkSessionHelperUtil import get_spark_session
import configparser as cp
import sys

from Writer import write_to_sink


def main():
    environment = sys.argv[1]
    source = sys.argv[2]
    sink = sys.argv[3]
    app_name = "RetailAnalytics"
    spark = get_spark_session(app_name)
    props = cp.RawConfigParser()
    props.read("application.properties")

    spark.conf.set("spark.sql.shuffle.partitions", "2")

    input_base_dir = props.get(environment, INPUT_BASE_DIR)
    source_file_format = props.get(source, SOURCE_FILE_FORMAT)

    orders_data_folder = props.get(source, ORDERS_DATA_PATH)
    orders_custom_schema = OrdersSchema.orders_schema
    orders_df = read_from_source(
        spark, input_base_dir, source_file_format, orders_data_folder, orders_custom_schema)

    order_items_data_folder = props.get(source, ORDER_ITEMS_DATA_PATH)
    order_items_custom_schema = OrderItemsSchema.order_items_schema
    order_items_df = read_from_source(spark, input_base_dir, source_file_format, order_items_data_folder,
                                      order_items_custom_schema)

    final_processed_df = get_daily_revenue(orders_df, order_items_df)

    # final_processed_df.show()

    output_base_dir = props.get(environment, OUTPUT_BASE_DIR)
    sink_file_format = props.get(sink, SINK_FILE_FORMAT)
    sink_output_mode = props.get(sink, SINK_OUTPUT_MODE)
    output_folder_name = props.get(sink, SINK_OUTPUT_PROCESSED_FOLDER_NAME)

    write_to_sink(final_processed_df, sink_file_format,
                  sink_output_mode, output_folder_name, output_base_dir)


if __name__ == "__main__":
    main()
