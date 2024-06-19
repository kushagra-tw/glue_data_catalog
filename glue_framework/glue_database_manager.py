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
