
import exdemon.endpoints.metric as metric
import exdemon.endpoints.monitor as monitor
import exdemon.endpoints.schema as schema

class Api():
    def __init__(self, app):
        # Metrics endpoints
        app.add_url_rule('/api/v1/metrics/<int:id>', methods=['GET','PUT','DELETE'],
            defaults={'project': None, 'environment': None, 'name': None},
            view_func=metric.MetricEndpoint.as_view('metric_id_operations'), 
        )
        app.add_url_rule('/api/v1/metrics/<project>/<environment>/<name>', methods=['GET','PUT','DELETE'],
            defaults={'id': None},
            view_func=metric.MetricEndpoint.as_view('metric_name_operations'),
        )
        app.add_url_rule('/api/v1/metrics', methods=['GET','POST'],
            defaults={'id': None, 'project': None, 'environment': None, 'name': None},
            view_func=metric.MetricEndpoint.as_view('metric_collection')
        )

        # Monitors endpoints
        app.add_url_rule('/api/v1/monitors/<int:id>', methods=['GET','PUT','DELETE'],
            defaults={'project': None, 'environment': None, 'name': None},
            view_func=monitor.MonitorEndpoint.as_view('monitor_id_operations'), 
        )
        app.add_url_rule('/api/v1/monitors/<project>/<environment>/<name>', methods=['GET','PUT','DELETE'],
            defaults={'id': None},
            view_func=monitor.MonitorEndpoint.as_view('monitor_name_operations'),
        )
        app.add_url_rule('/api/v1/monitors', methods=['GET','POST'],
            defaults={'id': None, 'project': None, 'environment': None, 'name': None},
            view_func=monitor.MonitorEndpoint.as_view('monitor_collection')
        )

        # Schemas endpoints
        app.add_url_rule('/api/v1/schemas/<int:id>', methods=['GET','PUT','DELETE'],
            defaults={'project': None, 'environment': None, 'name': None},
            view_func=schema.SchemaEndpoint.as_view('schema_id_operations'), 
        )
        app.add_url_rule('/api/v1/schemas/<project>/<environment>/<name>', methods=['GET','PUT','DELETE'],
            defaults={'id': None},
            view_func=schema.SchemaEndpoint.as_view('schema_name_operations'),
        )
        app.add_url_rule('/api/v1/schemas', methods=['GET','POST'],
            defaults={'id': None, 'project': None, 'environment': None, 'name': None},
            view_func=schema.SchemaEndpoint.as_view('schema_collection')
        )

