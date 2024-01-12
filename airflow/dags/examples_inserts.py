#insert example in postgres
def _insert():
    """
    Legacy function for inserting data into a PostgreSQL database.

    Note: This function is not currently in use.
    """
    
    df = pd.read_json('data/data_hiking.json')
    with open("/opt/airflow/dags/inserts.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS hikes;\n"
            "CREATE TABLE IF NOT EXISTS hikes (\n"
            "    Hike_name VARCHAR(255),\n"
            "    Ranking VARCHAR(255),\n"
            "    Difficulty VARCHAR(255),\n"
            "    Distance_km DECIMAL(10, 2),\n"
            "    Elevation_gain_m DECIMAL(10, 2),\n"
            "    Gradient VARCHAR(255),\n"
            "    Time_hours VARCHAR(255),\n"
            "    Dogs VARCHAR(10),\n"
            "    _4x4 VARCHAR(255),\n"
            "    Season VARCHAR(255),\n"
            "    Region VARCHAR(255)\n"
            ");\n"
        )

        for index, row in df.iterrows():
            hike_name = row['HIKE NAME']
            ranking = categorize_stars(row['RANKING'])
            difficulty = row['DIFFICULTY']
            distance_km = row['DISTANCE (KM)']
            elevation_gain_m = row['ELEVATION GAIN (M)']
            gradient = row['GRADIENT']
            time_hours = f"'{categorize_time_hours(row['TIME (HOURS)'])}'"
            dogs = row['DOGS']
            cars = row['4X4']
            season = row['SEASON']
            region = row['REGION']

            elevation_gain_m = elevation_gain_m.replace(',', '.')

            f.write(
                "INSERT INTO hikes VALUES ("
                f"'{hike_name}', '{ranking}', '{difficulty}', {distance_km}, {elevation_gain_m}, '{gradient}', {time_hours}, '{dogs}', '{cars}', '{season}', '{region}'"
                ");\n"
            )

        f.close()

#DAG
generate_script_hikes = PythonOperator(
    task_id='generate_insert',
    dag=dag,
    python_callable=_insert,
    trigger_rule='none_failed',
)

load_hikes = PostgresOperator(
    task_id='insert_inserts',
    dag=dag,
    postgres_conn_id='postgres_default',
    sql='inserts.sql',
    trigger_rule='none_failed',
    autocommit=True
)

#insert example in mongo
def insert_mongo():
    """
    Inserts hiking data into a MongoDB collection.
    """
    with open(os.path.join(DATA_DIR, 'data_hiking.json'), 'r') as json_file:
        hike_data_list = json.load(json_file)
    
    name_collection = 'hikes'
    if name_collection in mongo_db.list_collection_names():
        mongo_collection = mongo_db[name_collection]
        mongo_collection.drop()

    mongo_collection_hikes.insert_many(hike_data_list)

#DAG
add_hikes_to_mongo = PythonOperator(
task_id = "add_hikes_to_mongo",
dag = dag,
python_callable = insert_mongo,
depends_on_past=False,
trigger_rule='none_failed',
)