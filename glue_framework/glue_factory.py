import boto3
from glue_table_manager import TableManager
from glue_crawler_manager import CrawlerManager
from glue_database_manager import DatabaseManager

class GlueFactory:
    def __init__(self, region_name='us-east-1'):
        self.glue_client = boto3.client('glue', region_name=region_name)

    def create_database_manager(self):
        return DatabaseManager(self.glue_client)

    def create_table_manager(self):
        return TableManager(self.glue_client)

    def create_crawler_manager(self):
        return CrawlerManager(self.glue_client)
