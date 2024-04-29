import yaml
import sshtunnel
import pandas as pd

from time import time, sleep
from sqlalchemy import create_engine

def transform(df: pd.DataFrame, mappingPath: str) -> pd.DataFrame:
    with open(mappingPath, "r", encoding='utf-8') as stream:
        try:
            mapping = yaml.safe_load(stream=stream)
            df = df.rename(columns=mapping['col_name'])
            return df.astype(dtype=mapping['dtype'])
        except yaml.YAMLError as yamlErr:
            print(yamlErr)

def main():
    with open("./config.yml", "r") as stream:
        try:
            config = yaml.safe_load(stream=stream)
        except yaml.YAMLError as yamlErr:
            print(yamlErr)
            
    # SETUP CONFIGURATION
    ec2Config = config['instances']['ec2']
    dbConfig = config['instances']['pg']
    srcConfig = config['csv2pg']['incremental_src']
    tgtConfig = config['csv2pg']['tgt']
    
    # ESTABLISH SSH CONNECTION
    with sshtunnel.SSHTunnelForwarder(
        (ec2Config['host']),
        ssh_username=ec2Config['username'],
        ssh_pkey=ec2Config['pemkey_path'],
        remote_bind_address=(dbConfig['host'], dbConfig['port'])
    ) as tunnel:
        print(f"SSH TUNNEL ESTABLISHED - {tunnel.is_active}")
        # CONNECT TO RDS
        engine = create_engine(f"{dbConfig['dbtype']}://{dbConfig['username']}:{dbConfig['password']}@localhost:{tunnel.local_bind_port}/{dbConfig['dbname']}")
        engine.connect()
        # INIT DATAFRAME ITERATOR
        # USE THIS ITERATOR TO INSERT CHUNK-BY-CHUNK DATA
        df_iter = pd.read_csv(
            srcConfig['path'],
            delimiter=',',
            encoding='utf-8',
            iterator=True,
            chunksize=1,
            index_col='row'
        )
        # INSERT THE DATA
        while True:
            try:
                start = time()
                df = next(df_iter)
                df = transform(df=df, mappingPath=srcConfig['dtype_mapping_path'])
                df.to_sql(
                    schema=tgtConfig['schema'],
                    name=tgtConfig['table'], 
                    con=engine, 
                    if_exists="append"
                )
                stop = time()
                print(f"Insert one row to {tgtConfig['schema']}.{tgtConfig['table']}, took {stop - start:.3f} seconds")
                sleep(1)
            except StopIteration:
                print(
                    f"Finish inserting {srcConfig['name']} into {tgtConfig['dbtype']}/{tgtConfig['name']}")
                break
        

if __name__ == "__main__":
    main()