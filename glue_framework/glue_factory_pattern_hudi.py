import boto3
import json

class GlueFactory:
    def __init__(self, region_name='us-east-1'):
        self.glue_client = boto3.client('glue', region_name=region_name)

    def create_database_manager(self):
        return DatabaseManager(self.glue_client)

    def create_table_manager(self):
        return TableManager(self.glue_client)

    def create_crawler_manager(self):
        return CrawlerManager(self.glue_client)

class DatabaseManager:
    def __init__(self, glue_client):
        self.glue_client = glue_client

    def database_exists(self, database_name):
        try:
            self.glue_client.get_database(Name=database_name)
            return True
        except self.glue_client.exceptions.EntityNotFoundException:
            return False

    def create_database(self, database_name, description='Database for the EDP data lake layers'):
        if not self.database_exists(database_name):
            try:
                self.glue_client.create_database(
                    DatabaseInput={
                        'Name': database_name,
                        'Description': description,
                    }
                )
                print(f"Database '{database_name}' created successfully.")
            except self.glue_client.exceptions.AlreadyExistsException:
                print(f"Database '{database_name}' already exists.")
        else:
            print(f"Database '{database_name}' already exists.")

    def update_database(self, database_name, new_description):
        try:
            self.glue_client.update_database(
                Name=database_name,
                DatabaseInput={
                    'Name': database_name,
                    'Description': new_description
                }
            )
            print(f"Database '{database_name}' updated successfully.")
        except self.glue_client.exceptions.EntityNotFoundException:
            print(f"Database '{database_name}' does not exist.")

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
                        'InputFormat': 'org.apache.hudi.hadoop.HoodieParquetInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                            'Parameters': {
                                'serialization.format': '1'
                            }
                        },
                        'Parameters': parameters,
                    },
                    'PartitionKeys': [
                        {
                            'Name': 'category',
                            'Type': 'string',
                            'Comment': 'year'
                        }],
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'EXTERNAL': 'TRUE',
                        'has_encrypted_data': 'false',
                        'hudi.metadata-listing-enabled': 'TRUE'
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
                print('exception')
        else:
            print('Table does not exist')

class CrawlerManager:
    def __init__(self, glue_client):
        self.glue_client = glue_client

    def crawler_exists(self, crawler_name):
        try:
            self.glue_client.get_crawler(Name=crawler_name)
            return True
        except self.glue_client.exceptions.EntityNotFoundException:
            return False

    def create_glue_crawler(self,crawler_name, database_name, s3_target_path):
        try:
            response = self.glue_client.create_crawler(
                Name=crawler_name,
                Role='arn:aws:iam::160071257600:role/service-role/AWSGlueServiceRole-glodlayerv2',
                DatabaseName=database_name,
                Description='Crawler for S3 data',
                Targets={
                    'S3Targets': [
                        {
                            'Path': s3_target_path,
                            'Exclusions': ['**/*.tmp', '**/__temporary/*']  # Add exclusions if needed
                        },
                    ],
                },
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                },
                Configuration=json.dumps({
                    'Version':1.0,
                    "CrawlerOutput": {
                        "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}
                    }
                }),
                RecrawlPolicy={
                    'RecrawlBehavior': 'CRAWL_EVERYTHING'
                },
                # TablePrefix='your_table_prefix_',  # Optional table prefix for the tables created
                Tags={
                    'Purpose': 'DataCatalog'
                }
            )
            print(f"Crawler '{crawler_name}' created successfully.")
        except self.glue_client.exceptions.AlreadyExistsException:
            print(f"Crawler '{crawler_name}' already exists.")
        except Exception as e:
            print(f"Error creating crawler: {e}")

    def create_crawler_using_glue_table(self, crawler_name, database_name, table_name):
        if not self.crawler_exists(crawler_name):
            try:
                self.glue_client.create_crawler(
                    Name=crawler_name,
                    Role='arn:aws:iam::160071257600:role/service-role/AWSGlueServiceRole-glodlayerv2',
                    DatabaseName=database_name,
                    Targets={
                        'CatalogTargets': [
                            {
                                'DatabaseName': database_name,
                                'Tables': [table_name]
                            },
                        ],
                    },
                    SchemaChangePolicy={
                        'UpdateBehavior': 'LOG',
                        'DeleteBehavior': 'LOG'
                    },
                    RecrawlPolicy={'RecrawlBehavior': 'CRAWL_EVERYTHING'}
                )
                print(f"Crawler '{crawler_name}' created successfully.")
            except self.glue_client.exceptions.AlreadyExistsException:
                print(f"Crawler '{crawler_name}' already exists.")
        else:
            print(f"Crawler '{crawler_name}' already exists.")

    def run_crawler(self,crawler_name):
        self.glue_client.start_crawler(Name=crawler_name)

def main():
    # Initialize GlueFactory
    glue_factory = GlueFactory()

    # Database Manager
    database_manager = glue_factory.create_database_manager()
    database_name = 'jcp-edp-gold'
    database_manager.create_database(database_name)


    with open('./hudi_table_schema.json') as file:
        schema_data = json.loads(file.read())
    columns = [{"Name": field["name"], "Type": field["type"]} for field in schema_data["fields"]]
    print(columns)

    # s3_target_path = 's3://jcp-edp-dev-bronze-test/customer/cdm/customer_identity'
    # s3_target_path = 's3://jcp-edp-dev-silver-test/customer/cdm/customer_identity/'
    s3_target_path = 's3://jcp-edp-dev-bronze-test/data/sample_hudi_cow_table'


    # Table Manager
    table_manager = glue_factory.create_table_manager()
    table_name = 'cdm_customer_identity'
    # columns = [
    #     {"Name": "customer_id", "Type": "string"},
    #     {"Name": "name", "Type": "string"},
    #     {"Name": "email", "Type": "string"},
    #     {"Name": "created_at", "Type": "timestamp"}
    # ]
    location = s3_target_path
    file_format = 'hudi'
    common_parameters = {
        "classification": file_format,
        "compressionType": "gzip" if file_format == "parquet" else 'None',
    }
    table_manager.create_table(database_name, table_name, columns, location, file_format, common_parameters)

    # # Crawler Manager
    crawler_manager = glue_factory.create_crawler_manager()
    #
    #
    # # crawler_manager.create_glue_crawler('my_crawler',database_name, s3_target_path)
    #
    crawler_manager.create_crawler_using_glue_table('gold_layer_crawler',database_name,table_name)
    #


    # print('running crawler')
    crawler_manager.run_crawler('gold_layer_crawler')
if __name__ == "__main__":
    main()
