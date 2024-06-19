class TableManager:
    def __init__(self, glue_client):
        self.glue_client = glue_client

    def table_exists(self, database_name, table_name):
        try:
            self.glue_client.get_table(DatabaseName=database_name, Name=table_name)
            return True
        except self.glue_client.exceptions.EntityNotFoundException:
            return False

    def create_table(self, database_name, table_name, columns, location, file_format, parameters):
        if self.table_exists(database_name, table_name):
            print(f"Table '{table_name}' already exists in database '{database_name}'.")
            return

        try:
            self.glue_client.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': {
                        'Columns': columns,
                        'Location': location,
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' if file_format == 'parquet' else 'org.apache.hudi.hadoop.HoodieParquetInputFormat',
                            'Parameters': {
                                'serialization.format': '1'
                            }
                        },
                        'Parameters': parameters,
                    },
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'EXTERNAL': 'TRUE',
                        'has_encrypted_data': 'false'
                    }
                }
            )
            print(f"Table '{table_name}' created successfully in database '{database_name}'.")
        except self.glue_client.exceptions.AlreadyExistsException:
            print(f"Table '{table_name}' already exists in database '{database_name}'.")

    def update_table(self, database_name, table_name, new_location, new_parameters):
        if self.table_exists(database_name, table_name):
            try:
                self.glue_client.update_table(
                    DatabaseName=database_name,
                    TableInput={
                        'Name': table_name,
                        'StorageDescriptor': {
                            'Location': new_location,
                            'Parameters': new_parameters,
                        }
                    }
                )
                print(f"Table '{table_name}' updated successfully in database '{database_name}'.")
            except self.glue_client.exceptions.EntityNotFoundException:
                print(f"Table '{table_name}' does not exist in database '{database_name}'.")
