MYSQL_CONFIG = {
    'host': '217.61.57.46',
    'port': 3306,
    'user': 'neo_data_admin',
    'password': 'Proyahaxuqithab9oplp',
    'database': 'neo_data'
}

KAFKA_CONFIG = {
    'bootstrap_servers': '77.81.230.104:9092',
    'input_topic': 'athlete_event_results_maslianko_andrii',
    'output_topic': 'processed_athlete_stats_maslianko_andrii',
    'security_protocol': 'SASL_PLAINTEXT',
    'sasl_mechanism': 'PLAIN',
    'username': 'admin',
    'password': 'VawEzo1ikLtrA8Ug8THa',
    'sasl_jaas_config': "org.apache.kafka.common.security.plain.PlainLoginModule required username=admin password=VawEzo1ikLtrA8Ug8THa;"
}