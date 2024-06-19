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

    def create_crawler_using_glue_table(self, crawler_conf):
        print('inside glue crawler manager',crawler_conf)
        if not self.crawler_exists(crawler_conf['Name']):
            try:
                self.glue_client.create_crawler(
                    Name=crawler_conf['Name'],
                    Role=crawler_conf['Role'],
                    DatabaseName=crawler_conf['DatabaseName'],
                    Targets=crawler_conf['Targets'],
                    SchemaChangePolicy=crawler_conf['SchemaChangePolicy'],
                    RecrawlPolicy=crawler_conf['RecrawlPolicy']
                )
                print(f"Crawler '{crawler_conf['Name']}' created successfully.")
            except self.glue_client.exceptions.AlreadyExistsException:
                print(f"Crawler '{crawler_conf['Name']}' already exists.")
        else:
            print(f"Crawler '{crawler_conf['Name']}' already exists.")

    def run_crawler(self,crawler_name):
        self.glue_client.start_crawler(Name=crawler_name)
