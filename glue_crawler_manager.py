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
                Role='arn:aws:iam::160071257600:role/aws_glue_new_role',
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
                    Role='arn:aws:iam::160071257600:role/service-role/AWSGlueServiceRole-JCPCrawler',
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
                        'UpdateBehavior': 'UPDATE_IN_DATABASE',
                        'DeleteBehavior': 'LOG'
                    },
                    RecrawlPolicy={'RecrawlBehavior': 'CRAWL_EVERYTHING'}
                )
                print(f"Crawler '{crawler_name}' created successfully.")
            except self.glue_client.exceptions.AlreadyExistsException:
                print(f"Crawler '{crawler_name}' already exists.")
        else:
            print(f"Crawler '{crawler_name}' already exists.")
