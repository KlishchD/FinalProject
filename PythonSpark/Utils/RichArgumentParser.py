from argparse import ArgumentParser


class RichArgumentParser(ArgumentParser):
    def add_mode(self):
        self.add_argument("mode", type=str, help="Mode in which app will run (dev or prod)")
        return self

    def add_dynamic_table_data_source(self, name):
        self.add_argument(f"{name}_filepath", type=str, help=f"Path to a file with {name}")
        return self

    def add_static_table_data_source(self, name):
        self.add_argument(f"--{name}_filepath", dest=f"{name}_filepath", type=str,
                          help=f"Filepath to a file with {name}")
        self.add_argument(f"--{name}_redis_keys_pattern", dest=f"{name}_redis_keys_pattern", type=str,
                          help=f"Keys pattern of {name}")
        self.add_argument(f"--{name}_redis_key_column", dest=f"{name}_redis_key_column", type=str,
                          help=f"Key column of {name}")
        return self

    def add_file_format_to_data(self, name):
        self.add_argument(f"{name}_file_format", type=str, help=f"FIle format of {name} data")
        return self

    def add_read_service_account(self):
        self.add_argument(f"--read_service_account_email", dest="read_service_account_email", type=str,
                          help="Email of a service account for reading from GCS")
        self.add_argument(f"--read_service_account_key_filepath", dest="read_service_account_key_filepath", type=str,
                          help="Filepath to a service account credentials for reading from GCS")
        return self

    def add_write_service_account(self):
        self.add_argument(f"--write_service_account_email", dest="write_service_account_email", type=str,
                          help="Email of a service account for writing in GCS")
        self.add_argument(f"--write_service_account_key_filepath", dest="write_service_account_key_filepath", type=str,
                          help="Filepath to a service account credentials for writing in GCS")
        return self

    def add_result_filepath(self):
        self.add_argument(f"--result_filepath", dest="result_filepath", type=str,
                          help="Path where to write results")
        return self

    ##TODO:FIX array types
    def add_locations(self):
        self.add_argument(f"--locations", dest="locations", type=list, default=None,
                          help="List of locations to include in aggregation (omit if you want to get aggregation for all locations")
        return self

    def add_devices(self):
        self.add_argument(f"--devices", dest="devices", type=list, default=None,
                          help="List of devices to include in aggregation (omit if want to get aggregation for all devices)")
        return self

    def add_time_frame(self):
        self.add_argument("--time_frame", dest="time_frame", type=list, default=None,
                          help="Time to include in aggregation (omit if you want to get aggregation all time)")
        return self

    def add_postgres(self):
        self.add_argument("--postgres_url", dest="postgres_url", type=str,
                          help="JDBC url of postgres")
        self.add_argument("--postgres_user", dest="postgres_user", type=str,
                          help="Postgres user")
        self.add_argument("--postgres_password", dest="postgres_password", type=str,
                          help="Postgres user password")
        return self

    def add_redis(self):
        self.add_argument("--redis_host", dest="redis_host", type=str,
                          help="Host of redis")
        self.add_argument("--redis_prot", dest="redis_port", type=str,
                          help="Port of redis")
        return self

    def add_big_query(self):
        self.add_argument("--big_query_service_account_key_filepath", dest="big_query_service_account_key_filepath",
                          type=str, help="Path to a file with service account credential to write in BigQuery")
        self.add_argument("--temporary_bucket_name", dest="temporary_bucket_name", type=str,
                          help="Temporary bucket name for Big Query")
        return self

    def add_result_table(self):
        self.add_argument("--results_table", dest="results_table", type=str,
                          help="Table to write results in")
        return self
