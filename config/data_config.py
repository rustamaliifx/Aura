from typing import List, Optional 
from pydantic_settings import BaseSettings

class DataConfig(BaseSettings):
    # Elasticsearch settings
    HOST: str = "localhost"
    PORT: int = 3432
    USER: str = "aliru"
    PASSWORD: str = "aliru"

    # Index settings
    INDEX_NAME: str = "log-app-jamarequest*"
    OUTPUT_INDEX_NAME: str = "log-app-jamarequest-processed"

    # Data processing settings
    COLUMNS_TO_USE = ['@timestamp', '@version', 'HA_active_connections',
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
    
    COLUMNS_TO_DROP = ['@version', 'HA_proxy', 'HA_pid', 'priority', 'fields.component', 'host', 'tags', 'HA_http_version', 'ifx-index', 'timestamp']

    COLUMNS_TIMESTAMP_FEATURES = ['@timestamp', 't_rec', 't_rec_cn']

    COLUMNS_CATEGORICAL_FEATURES = categorical_cols = ['HA_client_ip', 'HA_http_host', 'HA_http_method', 'HA_http_referer',
                'HA_http_request', 'HA_http_user_agent', 'HA_req_host',
                'HA_req_normalized', 'HA_session_id', 'HA_termination_state',
                'HA_user_identifier', 'HA_username', 'h_rec']
    
    COLUMNS_NUMERICAL_FEATURES = ['HA_active_connections', 'HA_backend_current_queue',
                'HA_backend_max_queue', 'HA_backend_queue_time', 'HA_bytes_read',
                'HA_client_ip', 'HA_client_port', 'HA_frontend_max_connections',
                'HA_http_host', 'HA_http_method', 'HA_http_referer', 'HA_http_request',
                'HA_http_user_agent', 'HA_queue_prio', 'HA_req_host',
                'HA_req_normalized', 'HA_req_order', 'HA_request_queue_time',
                'HA_response_code', 'HA_session_id', 'HA_termination_state',
                'HA_time_to_transfer', 'HA_total_request_time', 'HA_total_sessions',
                'HA_total_time', 'HA_user_identifier', 'HA_username', 'h_rec', 'port']

dataconfig = DataConfig() 

