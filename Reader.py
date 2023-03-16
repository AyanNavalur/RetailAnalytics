def read_from_source(spark, input_base_dir, file_format, input_folder_name, custom_schema):
    return (spark
            .read
            .format(file_format)
            .schema(custom_schema)
            .load(f'{input_base_dir}/{input_folder_name}'))
