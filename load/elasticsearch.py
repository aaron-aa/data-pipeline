from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


class _CompressedConnection(Urllib3HttpConnection):

    def __init__(self, *args, **kwargs):
        super(_CompressedConnection, self).__init__(*args, **kwargs)
        self.headers.update(urllib3.make_headers(accept_encoding=True))


def get_es_connection_config(schema, mode, settings, compress=False):
    conn_config = {'verify_certs': False}
    if mode == 'es-hadoop':
        conn_config['es.nodes'] = getattr(settings, schema['es_host'])
        if schema.get('es.net.ssl', False):
            conn_config['es.net.ssl'] = True
            key_settings = [
                'es.net.http.auth.user',
                'es.net.http.auth.pass',
                'es.net.ssl.keystore.location',
                'es.net.ssl.truststore.location'
            ]
            for key_setting in key_settings:
                if key_setting in schema:
                    conn_config[key_setting] = getattr(settings, schema[key_setting])
    elif mode == 'es-py':
        conn_config['max_retries'] = 10
        conn_config['retry_on_timeout'] = True
        conn_config['hosts'] = getattr(settings, schema['es_host']).split(',')

        if schema.get('es.net.basic_auth', False):
            conn_config['connection_class'] = _CompressedConnection if compress else Urllib3HttpConnection
            conn_config['http_auth'] = (
                getattr(settings, schema['es.net.http.auth.user']),
                getattr(settings, schema['es.net.http.auth.pass'])
            )

        if getattr(settings, 'TEST', False):
            return conn_config

        if schema.get('es.net.ssl', False):
            conn_config['connection_class'] = _CompressedConnection if compress else Urllib3HttpConnection
            conn_config['use_ssl'] = True
            if 'es.net.http.auth.user' in schema and 'es.net.http.auth.pass' in schema:
                conn_config['http_auth'] = (
                    getattr(settings, schema['es.net.http.auth.user']),
                    getattr(settings, schema['es.net.http.auth.pass'])
                )
            if schema.get('verify_certs', False):
                conn_config['verify_certs'] = True
                if 'es.net.ssl.keystore.location' in schema:
                    conn_config['client_key'] = getattr(settings, 'es.net.ssl.keystore.location')
                if 'es.net.ssl.truststore.location' in schema:
                    conn_config['client_cert'] = getattr(settings, 'es.net.ssl.truststore.location')

    else:
        raise Exception("mode should be ['es-hadoop' or 'es-py']")
    return conn_config


def es_instance(schema_name="INT_REVIEWS_ES_O", timeout=10, compress=True):
    conn_conf = get_es_connection_config(schema(schema_name), 'es-py', settings, compress)
    conn_conf["timeout"] = timeout
    return Elasticsearch(**conn_conf)
