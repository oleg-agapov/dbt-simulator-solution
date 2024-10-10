import duckdb
import pathlib
import requests


def create_duck_db():
    
    months_to_download = ['01', '02', '03']
    trips_data_urls = [f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{month}.parquet' for month in months_to_download]
    
    lookup_data_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    lookup_data_filepath = pathlib.Path(__file__).parent /  'dbt_simulator' / 'seeds' / 'taxi_zone_lookup.csv'

    database_path = pathlib.Path(__file__).parent / 'dbt_simulator' / 'database.duckdb'
    
    with duckdb.connect(database_path) as con:
        con.execute(f"DROP TABLE IF EXISTS taxi_trips")
        con.execute(f"CREATE TABLE taxi_trips AS SELECT * FROM read_parquet('{trips_data_urls[0]}')")
        for url in trips_data_urls[1:]:
            con.execute(f"INSERT INTO taxi_trips SELECT * FROM read_parquet('{url}')")
        
        lookup_request = requests.get(lookup_data_url)
        if lookup_request.status_code == 200:
            with open(lookup_data_filepath, 'wb') as file:
                file.write(lookup_request.content)

if __name__ == '__main__':
    create_duck_db()
