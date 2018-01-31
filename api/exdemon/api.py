
import exdemon.endpoints.metric as metric

class Api():
    def __init__(self, app):
        app.add_url_rule('/api/v1/metric', view_func=metric.MetricEndpoint.as_view('metric'), methods=['GET','POST','PUT','DELETE'])
        #app.add_url_rule('/api/v1/schema', view_func=TestApi.as_view('schema'), methods=['GET','POST','PUT','DELETE'])
        #app.add_url_rule('/api/v1/monitor', view_func=TestApi.as_view('monitor'), methods=['GET','POST','PUT','DELETE'])
