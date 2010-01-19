import datetime

from libcloud.drivers.ec2 import EC2Connection, EC2NodeDriver
from libcloud.drivers import ec2

EC2_CLOUD_WATCH_HOST = 'monitoring.amazonaws.com'
METRICS = ('CPUUtilization', 'NetworkIn', 'NetworkOut', 'DiskReadBytes', 'DiskWriteBytes')
STATISTICS_TYPE = 'Average'

# Resetting this here, fine for now if this is 
# only simply performing CloudWatch operations

# Warning -- do not use this in conjunction within 
# an app relying on the actual EC2 provider, as this 
# changes the API URL!
ec2.NAMESPACE = 'http://monitoring.amazonaws.com/doc/2009-05-15/'

class Metric(object):

    def __init__(self, id, measure_name):
        self.id = id
        self.measure_name = measure_name

    def __repr__(self):
        return '%s for %s' % (self.measure_name, self.id)

class MetricStatistic(object):

    def __init__(self, metric, timestamp, unit, value):
        self.metric = metric
        self.timestamp = timestamp
        self.unit = unit
        self.value = value

    def __repr__(self):
        return '(%s) %s: %s %s' % (self.timestamp, self.metric, self.value, self.unit)

class EC2CloudWatchConnection(EC2Connection):

    host = EC2_CLOUD_WATCH_HOST

class EC2CloudWatchNodeDriver(EC2NodeDriver):

    connectionCls = EC2CloudWatchConnection

    def list_metrics(self, id):
        params = {'Action': 'ListMetrics'}

        metrics = self._to_metrics(
                    self.connection.request('/', params=params).object,
                    'ListMetricsResult/Metrics/member', id)
        return metrics

    def _to_metrics(self, object, xpath, id):
        metrics = []
        parents = object.findall(self._fixxpath(xpath))

        for el in parents:
            metrics = metrics + [ self._to_metric(el, id)
                                      for props in el.findall(
                                          self._fixxpath('Dimensions/member'))
                                          # ElementTree supports only a small subset of XPATH,
                                          # so we do this comparison manually
                                              if self._findtext(props, 'Value') == id ]
        return metrics

    def _to_metric(self, element, id):
        return Metric(id, self._findtext(element, 'MeasureName'))

    def metric_statistic(self, id, start_date, end_date, metric):
        params = {
            'Action': 'GetMetricStatistics',
            'Period': '60', # This is the default, but included for clarity
            'Statistics.member.1': STATISTICS_TYPE,
            'Namespace': 'AWS/EC2',
            'StartTime': self._to_iso_8601(start_date),
            'EndTime': self._to_iso_8601(end_date),
            'MeasureName': metric,
            'Dimensions.member.1.Name': 'InstanceId',
            'Dimensions.member.1.Value': id
        }
        return self._to_metric_statistics(
                   self.connection.request('/', params=params).object,
                   'GetMetricStatisticsResult/Datapoints/member', metric)

    def _to_iso_8601(self, date):
        return date.strftime('%Y-%m-%dT%H:%M:%S')

    def _to_metric_statistics(self, object, xpath, metric):
        stats = []
        stats = stats + [ self._to_metric_statistic(el, metric)
                              for el in object.findall(self._fixxpath(xpath)) ]
        return stats

    def _to_metric_statistic(self, element, metric):
        return MetricStatistic(metric, self._findtext(element, 'Timestamp'),
                               self._findtext(element, 'Unit'),
                               float(self._findtext(element, STATISTICS_TYPE)))

    def all_metric_statistics(self, id, start_date, end_date):
        return self.metric_statistics_of_type(id, start_date, end_date, types=METRICS)

    def metric_statistics_of_type(self, id, start_date, end_date, types=None):
        data = {}
        for metric in types:
            metrics = self.metric_statistic(id, start_date, end_date, metric)

            if not metrics:
                continue

            data[metric] = metrics
        return data
