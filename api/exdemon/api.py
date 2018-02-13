
import exdemon.endpoints.metric as metric

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

