from typing import List, Optional
from pydantic_settings import BaseSettings

class DataConfig(BaseSettings):
    # Elasticsearch settings
    HOST: str = "esdev01api.vih.infineon.com"
    PORT: int = 9200

    # Index settings
    INDEX_NAME: str = "log-app-jamarequest--*"
    OUTPUT_INDEX_NAME: str = "log-app-jamarequest-processed"
    BATCH_SIZE: int = 500 # Number of records to fetch in each batch 
    PROCESSING_INTERVAL: int = 60 # in seconds 

    # Data processing settings
    COLUMNS_TO_USE: List = ['@timestamp', '@version', 'HA_active_connections',
                'HA_backend_current_queue', 'HA_backend_max_queue',
                'HA_backend_queue_time', 'HA_bytes_read', 'HA_client_ip',
                'HA_client_port', 'HA_frontend_max_connections', 'HA_http_host',
                'HA_http_method', 'HA_http_referer', 'HA_http_request',
                'HA_http_user_agent', 'HA_http_version', 'HA_pid', 'HA_proxy',
                'HA_queue_prio', 'HA_req_host', 'HA_req_normalized', 'HA_req_order',
                'HA_request_queue_time', 'HA_response_code', 'HA_session_id',
                'HA_termination_state', 'HA_time_to_transfer', 'HA_total_request_time',
                'HA_total_sessions', 'HA_total_time', 'HA_user_identifier',
                'HA_username', 'fields.component', 'h_rec', 'host', 'ifx-index', 'port',
                'priority', 't_rec', 't_rec_cn', 'tags', 'timestamp']
    
    COLUMNS_TO_DROP: List = ['@version', 'HA_proxy', 'HA_pid', 'priority', 'fields.component', 'host', 'tags', 'HA_http_version', 'ifx-index', 'timestamp']

    COLUMNS_TIMESTAMP_FEATURES: List = ['@timestamp', 't_rec', 't_rec_cn']

    COLUMNS_CATEGORICAL_FEATURES: List = ['HA_client_ip', 'HA_http_host', 'HA_http_method', 'HA_http_referer',
                'HA_http_request', 'HA_http_user_agent', 'HA_req_host',
                'HA_req_normalized', 'HA_session_id', 'HA_termination_state',
                'HA_user_identifier', 'HA_username', 'h_rec']
    
    COLUMNS_NUMERICAL_FEATURES: List = ['HA_active_connections', 'HA_backend_current_queue',
                'HA_backend_max_queue', 'HA_backend_queue_time', 'HA_bytes_read',
                'HA_client_ip', 'HA_client_port', 'HA_frontend_max_connections',
                'HA_http_host', 'HA_http_method', 'HA_http_referer', 'HA_http_request',
                'HA_http_user_agent', 'HA_queue_prio', 'HA_req_host',
                'HA_req_normalized', 'HA_req_order', 'HA_request_queue_time',
                'HA_response_code', 'HA_session_id', 'HA_termination_state',
                'HA_time_to_transfer', 'HA_total_request_time', 'HA_total_sessions',
                'HA_total_time', 'HA_user_identifier', 'HA_username', 'h_rec', 'port']
    
    COLUMNS_MODEL_USE: List = ['HA_active_connections', 'HA_backend_current_queue',
                'HA_backend_max_queue', 'HA_backend_queue_time', 'HA_bytes_read',
                'HA_client_ip', 'HA_client_port', 'HA_frontend_max_connections',
                'HA_http_host', 'HA_http_method', 'HA_http_referer', 'HA_http_request',
                'HA_http_user_agent', 'HA_queue_prio', 'HA_req_host',
                'HA_req_normalized', 'HA_req_order', 'HA_request_queue_time',
                'HA_response_code', 'HA_session_id', 'HA_termination_state',
                'HA_time_to_transfer', 'HA_total_request_time', 'HA_total_sessions',
                'HA_total_time', 'HA_user_identifier', 'HA_username', 'h_rec', 'port',
                'year', 'month', 'day', 'hour', 'weekday', 'minute', 'second',
                't_rec_year', 't_rec_month', 't_rec_day', 't_rec_hour', 't_rec_weekday',
                't_rec_minute', 't_rec_second', 't_rec_cn_year', 't_rec_cn_month',
                't_rec_cn_day', 't_rec_cn_hour', 't_rec_cn_weekday', 't_rec_cn_minute',
                't_rec_cn_second']

dataconfig = DataConfig() 

