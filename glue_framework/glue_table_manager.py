class TableManager:
    def __init__(self, glue_client):
        self.glue_client = glue_client

    def table_exists(self, database_name, table_name):
        try:
            self.glue_client.get_table(DatabaseName=database_name, Name=table_name)
            return True
        except self.glue_client.exceptions.EntityNotFoundException:
            return False

    def create_table(self, table_config):
        if self.table_exists(table_config['DatabaseName'], table_config['TableInput']['Name']):
            print(f"Table '{table_config['TableInput']['Name']}' already exists in database '{table_config['DatabaseName']}'.")
            return

        try:
            self.glue_client.create_table(
                DatabaseName=table_config['DatabaseName'],
                TableInput=table_config['TableInput']
            )
            print(f"Table '{table_config['TableInput']['Name']}' created successfully in database '{table_config['DatabaseName']}'.")
        except self.glue_client.exceptions.AlreadyExistsException:
            print(f"Table '{table_config['TableInput']['Name']}' already exists in database '{table_config['DatabaseName']}'.")

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
