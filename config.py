from kedro.config import ConfigLoader

# Load the configuration
conf_loader = ConfigLoader(config_file="conf/base")

# Load the database credentials
db_credentials = conf_loader.get("credentials.yml")["db_credentials"]
