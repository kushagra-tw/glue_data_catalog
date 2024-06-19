import boto3
import json
import glue_factory
from json_parser import JsonParser


def main(database_name,table_name,crawler_name,table_type,schema_path,crawler_type):
    #using glue factory to generate the objects for all the manager..
    glue_factory_obj = glue_factory.GlueFactory('us-east-1')
    glue_database_manager = glue_factory_obj.create_database_manager()
    glue_crawler_manager = glue_factory_obj.create_crawler_manager()
    glue_table_manager = glue_factory_obj.create_table_manager()
    json_parser_obj = JsonParser()

    if glue_database_manager.database_exists(database_name):
        print('Database exist')
    else:
        glue_database_manager.create_database(database_name)

    #check provided glue table exist if not create it
    if glue_table_manager.table_exists(database_name,table_name):
        print('Table_exist')
    else:
        table_conf = json_parser_obj.parse_config('config/glue_table_config.json')
        schema = json_parser_obj.parse_config(schema_path)
        if table_type == "hudi_table":
            columns = [{"Name": field["name"].replace(' ','_'), "Type": field["type"]} for field in schema["fields"]]
        else:
            columns = [{"Name": field["name"], "Type": field["type"]} for field in schema["fields"]]
        table_conf[table_type]['TableInput']['StorageDescriptor']['Columns']=columns
        table_conf[table_type]['DatabaseName'] = database_name
        table_conf[table_type]['TableInput']['Name'] = table_name
        print(table_conf[table_type])
        glue_table_manager.create_table(table_conf[table_type])

    # #check if crawler exist if not create it
    if glue_crawler_manager.crawler_exists(crawler_name):
        print('crawler exist')
    else:
        crawler_conf = json_parser_obj.parse_config('config/glue_crawler_config.json')
        crawler_conf[crawler_type]['Name'] = crawler_name
        crawler_conf[crawler_type]['DatabaseName'] = database_name
        crawler_conf[crawler_type]['Targets']['CatalogTargets'][0]['DatabaseName'] = database_name
        crawler_conf[crawler_type]['Targets']['CatalogTargets'][0]['Tables'].append(table_name)
        glue_crawler_manager.create_crawler_using_glue_table(crawler_conf[crawler_type])


    #running crawler
    glue_crawler_manager.run_crawler(crawler_name)

if __name__ == "__main__":
    main('jcp-edp-bronze','cdm_customer_identity_test','bronze_layer_customer_identity_crawl','parquet_table','schema/customer_identity.json','catalog_crawler')



