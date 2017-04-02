# (C) Copyright 2017 Hewlett Packard Enterprise Development LP
import json
import logging
import requests
import six

from monasca_agent.collector import checks
from monasca_agent.collector.checks import utils

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 5
DEFAULT_KUBELET_PORT = "10255"
DEFAULT_CADVISOR_PORT = "4194"
CADVISOR_METRIC_URL = "/api/v2.0/stats?type=docker&recursive=true&count=1"
CADVISOR_SPEC_URL = "/api/v2.0/spec?type=docker&recursive=true"
POD_PHASE = {"Succeeded": 0,
             "Running": 1,
             "Pending": 2,
             "Failed": 3,
             "Unknown": 4}
REPORT_CONTAINER_METRICS = False
CADVISOR_METRICS = {
    "cpu_metrics": {
        "system": "cpu.system_time",
        "total": "cpu.total_time",
        "user": "cpu.user_time"
    },
    "memory_metrics": {
        "rss": "mem.rss_bytes",
        "swap": "mem.swap_bytes",
        "cache": "mem.cache_bytes",
        "usage": "mem.used_bytes",
        "failcnt": "mem.fail_count",
    },
    "filesystem_metrics": {
        "capacity": "fs.total_bytes",
        "usage": "fs.usage_bytes",
        "writes_completed": "fs.writes",
        "reads_completes": "fs.reads",
        "io_in_progress": "fs.io_current"
    },
    "network_metrics": {
        "rx_bytes": "net.in_bytes",
        "tx_bytes": "net.out_bytes",
        "rx_packets": "net.in_packets",
        "tx_packets": "net.out_packets",
        "rx_dropped": "net.in_dropped_packets",
        "tx_dropped": "net.out_dropped_packets",
        "rx_errors": "net.in_errors",
        "tx_errors": "net.out_errors",
    }
}

# format: (cadvisor metric name, [metric types], [metric units])
METRIC_TYPES_UNITS = {
    "cpu.system_time": (["gauge", "rate"], ["core_seconds", "cores_seconds_per_second"]),
    "cpu.total_time": (["gauge", "rate"], ["core_seconds", "cores_seconds_per_second"]),
    "cpu.user_time": (["gauge", "rate"], ["core_seconds", "cores_seconds_per_second"]),
    "mem.rss_bytes": (["gauge"], ["bytes"]),
    "mem.swap_bytes": (["gauge"], ["bytes"]),
    "mem.cache_bytes": (["gauge"], ["bytes"]),
    "mem.used_bytes": (["gauge"], ["bytes"]),
    "mem.fail_count": (["gauge"], ["count"]),
    "fs.total_bytes": (["gauge"], ["bytes"]),
    "fs.usage_bytes": (["gauge"], ["bytes"]),
    "fs.writes": (["gauge", "rate"], ["bytes", "bytes_per_second"]),
    "fs.reads": (["gauge", "rate"], ["bytes", "bytes_per_second"]),
    "fs.io_current": (["gauge"], ["bytes"]),
    "net.in_bytes": (["gauge", "rate"], ["bytes", "bytes_per_second"]),
    "net.out_bytes": (["gauge", "rate"], ["bytes", "bytes_per_second"]),
    "net.in_packets": (["gauge", "rate"], ["packets", "packets_per_second"]),
    "net.out_packets": (["gauge", "rate"], ["packets", "packets_per_second"]),
    "net.in_dropped_packets": (["gauge", "rate"], ["packets", "packets_per_second"]),
    "net.out_dropped_packets": (["gauge", "rate"], ["packets", "packets_per_second"]),
    "net.in_errors": (["gauge", "rate"], ["errors", "errors_per_second"]),
    "net.out_errors": (["gauge", "rate"], ["errors", "errors_per_second"])
}


class Kubernetes(checks.AgentCheck):
    """Queries Kubelet for metadata/health data and then cAdvisor for container metrics.
    """
    def __init__(self, name, init_config, agent_config, instances=None):
        checks.AgentCheck.__init__(self, name, init_config, agent_config, instances)
        if instances is not None and len(instances) > 1:
            raise Exception('Kubernetes check only supports one configured instance.')
        self.connection_timeout = int(init_config.get('connection_timeout', DEFAULT_TIMEOUT))
        self.host = None
        self.report_container_metrics = init_config.get('report_container_metrics', REPORT_CONTAINER_METRICS)
        self.kubernetes_connector = None

    def prepare_run(self):
        """Set up Kubernetes connection information"""
        instance = self.instances[0]
        self.host = instance.get("host", None)
        derive_host = instance.get("derive_host", False)
        if not self.host:
            if derive_host:
                self.kubernetes_connector = utils.KubernetesConnector(self.connection_timeout)
                self.host = self.kubernetes_connector.get_agent_pod_host()
            else:
                exception_message = "Either host or derive host must be set when " \
                                    "running Kubernetes plugin."
                self.log.exception(exception_message)
                raise Exception(exception_message)

    def check(self, instance):
        cadvisor, kubelet = self._get_urls(instance)
        kubernetes_labels = instance.get('kubernetes_labels', ["app"])
        container_dimension_map = {}
        pod_dimensions_map = {}
        dimensions = self._set_dimensions(None, instance)
        # Remove hostname from dimensions as the majority of the metrics are not tied to the hostname.
        del dimensions['hostname']
        kubelet_health_status = self._get_api_health("{}/healthz".format(kubelet))
        self.gauge("kubelet.health_status", 0 if kubelet_health_status else 1, dimensions=dimensions)
        try:
            pods = self._get_result("{}/pods".format(kubelet))
        except Exception as e:
            self.log.exception("Error getting data from kubelet - {}".format(e))
        else:
            self._process_pods(pods['items'],
                               kubernetes_labels,
                               dimensions,
                               container_dimension_map,
                               pod_dimensions_map)
            self._process_containers(cadvisor,
                                     dimensions,
                                     container_dimension_map,
                                     pod_dimensions_map)

    def _get_urls(self, instance):
        base_url = "http://{}".format(self.host)
        cadvisor_port = instance.get('cadvisor_port', DEFAULT_CADVISOR_PORT)
        kubelet_port = instance.get('kubelet_port', DEFAULT_KUBELET_PORT)
        cadvisor_url = "{}:{}".format(base_url, cadvisor_port)
        kubelet_url = "{}:{}".format(base_url, kubelet_port)
        return cadvisor_url, kubelet_url

    def _get_result(self, request_url, as_json=True):
        result = requests.get(request_url, timeout=self.connection_timeout)
        return result.json() if as_json else result

    def _get_api_health(self, health_url):
        try:
            result = self._get_result(health_url, as_json=False)
        except Exception as e:
            self.log.error("Error connecting to the health endpoint {} with exception {}".format(health_url, e))
            return False
        else:
            api_health = False
            for line in result.iter_lines():
                if line == 'ok':
                    api_health = True
                    break
            return api_health

    def _process_pods(self, pods, kubernetes_labels, dimensions, container_dimension_map, pod_dimensions_map):
        for pod in pods:
            pod_status = pod['status']
            pod_spec = pod['spec']
            pod_containers = pod_spec.get('containers', None)
            container_statuses = pod_status.get('containerStatuses', None)
            if not pod_containers or not container_statuses:
                # Pod does not have any containers assigned to it no-op going to next pod
                continue
            pod_dimensions = dimensions.copy()
            pod_dimensions.update(self._get_pod_dimensions(pod['metadata'], kubernetes_labels))
            pod_key = pod_dimensions['pod_name'] + pod_dimensions['namespace']
            pod_dimensions_map[pod_key] = pod_dimensions
            pod_retry_count = 0

            name2id = {}

            for container_status in container_statuses:
                container_restart_count = container_status['restartCount']
                container_dimensions = pod_dimensions.copy()
                container_name = container_status['name']
                container_dimensions['container_name'] = container_name
                container_dimensions['image'] = container_status['image']
                container_id = container_status.get('containerID', '').split('//')[-1]
                name2id[container_name] = container_id
                container_dimension_map[container_id] = container_dimensions
                if self.report_container_metrics:
                    container_ready = 0 if container_status['ready'] else 1
                    self.gauge("container.ready_status", container_ready, container_dimensions, hostname="SUPPRESS")
                    self.gauge("container.restart_count", container_restart_count, container_dimensions,
                               hostname="SUPPRESS")
                # getting an aggregated value for pod restart count
                pod_retry_count += container_restart_count

            # Report limit/request metrics
            if self.report_container_metrics:
                self._report_container_limits(pod_containers, container_dimension_map, name2id)

            self.gauge("pod.restart_count", pod_retry_count, pod_dimensions, hostname="SUPPRESS")
            self.gauge("pod.phase", POD_PHASE.get(pod_status['phase']), pod_dimensions, hostname="SUPPRESS")

    def _report_container_limits(self, pod_containers, container_dimension_map, name2id):
        for container in pod_containers:
            container_name = container['name']
            container_dimensions = container_dimension_map[name2id[container_name]]
            try:
                container_limits = container['resources']['limits']
                if 'cpu' in container_limits:
                    cpu_limit = container_limits['cpu']
                    cpu_value = self._convert_cpu_to_cores(cpu_limit)
                    self.gauge("container.cpu.limit", cpu_value, container_dimensions, hostname="SUPPRESS")
                if 'memory' in container_limits:
                    memory_limit = container_limits['memory']
                    memory_in_bytes = utils.convert_memory_string_to_bytes(memory_limit)
                    self.gauge("container.memory.limit_bytes", memory_in_bytes, container_dimensions,
                               hostname="SUPPRESS")
            except KeyError:
                self.log.exception("Unable to report container limits for {}".format(container_name))
            try:
                container_requests = container['resources']['requests']
                if 'cpu' in container_requests:
                    cpu_request = container_requests['cpu']
                    cpu_value = self._convert_cpu_to_cores(cpu_request)
                    self.gauge("container.request.cpu", cpu_value, container_dimensions, hostname="SUPPRESS")
                if 'memory' in container_requests:
                    memory_request = container_requests['memory']
                    memory_in_bytes = utils.convert_memory_string_to_bytes(memory_request)
                    self.gauge("container.request.memory_bytes", memory_in_bytes, container_dimensions,
                               hostname="SUPPRESS")
            except KeyError:
                self.log.exception("Unable to report container requests for {}".format(container_name))

    def _convert_cpu_to_cores(self, cpu_string):
        """Kubernetes reports cores in millicores in some instances.
        This method makes sure when we report on cpu they are all in cores
        """
        if "m" in cpu_string:
            cpu = float(cpu_string.split('m')[0])
            return cpu / 1000
        return float(cpu_string)

    def _get_pod_dimensions(self, pod_metadata, kubernetes_labels):
        pod_name = pod_metadata['name']
        pod_dimensions = {'pod_name': pod_name, 'namespace': pod_metadata['namespace']}
        if "labels" in pod_metadata:
            pod_labels = pod_metadata['labels']
            for label in kubernetes_labels:
                if label in pod_labels:
                    pod_dimensions[label] = pod_labels[label]
        # Get owner of pod to set as a dimension
        # Try to get from pod owner references
        pod_owner_references = pod_metadata.get('ownerReferences', None)
        if pod_owner_references:
            try:
                if len(pod_owner_references) > 1:
                    self.log.warn("More then one owner for pod {}".format(pod_name))
                pod_owner_reference = pod_owner_references[0]
                pod_owner_type = pod_owner_reference['kind']
                pod_owner_name = pod_owner_reference['name']
                self._set_pod_owner_dimension(pod_dimensions, pod_owner_type, pod_owner_name)
            except Exception:
                self.log.info("Could not get pod owner from ownerReferences for pod {}".format(pod_name))
        # Try to get owner from annotations
        else:
            try:
                pod_created_by = json.loads(pod_metadata['annotations']['kubernetes.io/created-by'])
                pod_owner_type = pod_created_by['reference']['kind']
                pod_owner_name = pod_created_by['reference']['name']
                self._set_pod_owner_dimension(pod_dimensions, pod_owner_type, pod_owner_name)
            except Exception:
                self.log.info("Could not get pod owner from annotations for pod {}".format(pod_name))
        return pod_dimensions

    def _get_deployment_name(self, pod_owner_name, pod_namespace):
        replica_set_endpoint = "/apis/extensions/v1beta1/namespaces/{}" \
                               "/replicasets/{}".format(pod_namespace,
                                                        pod_owner_name)
        try:
            replica_set = self.kubernetes_connector.get_request(replica_set_endpoint)
            replica_set_annotations = replica_set['metadata']['annotations']
            if "deployment.kubernetes.io/revision" in replica_set_annotations:
                return "-".join(pod_owner_name.split("-")[:-1])
        except Exception as e:
                self.log.warn("Could not connect to api to get replicaset data - {}".format(e))

    def _set_pod_owner_dimension(self, pod_dimensions, pod_owner_type, pod_owner_name):
        if pod_owner_type == "ReplicationController":
            pod_dimensions['replication_controller'] = pod_owner_name
        elif pod_owner_type == "ReplicaSet":
            if not self.kubernetes_connector:
                self.log.error("Can not set deployment name as connection information to API is not set."
                               " Setting ReplicaSet as dimension")
                deployment_name = None
            else:
                deployment_name = self._get_deployment_name(pod_owner_name, pod_dimensions['namespace'])
            if not deployment_name:
                pod_dimensions['replica_set'] = pod_owner_name
            else:
                pod_dimensions['deployment'] = deployment_name
        elif pod_owner_type == "DaemonSet":
            pod_dimensions['daemon_set'] = pod_owner_name
        else:
            self.log.info("Unsupported pod owner kind {} as a dimension for"
                          " pod {}".format(pod_owner_type, pod_dimensions))

    def _send_metrics(self, metric_name, value, dimensions, metric_types,
                      metric_units):
        for metric_type in metric_types:
            if metric_type == 'rate':
                dimensions.update({'unit': metric_units[
                    metric_types.index('rate')]})
                self.rate(metric_name + "_sec", value, dimensions,
                          hostname="SUPPRESS" if "pod_name" in dimensions else None)
            elif metric_type == 'gauge':
                dimensions.update({'unit': metric_units[
                    metric_types.index('gauge')]})
                self.gauge(metric_name, value, dimensions,
                           hostname="SUPPRESS" if "pod_name" in dimensions else None)

    def _parse_memory(self, memory_data, container_dimensions, pod_key, pod_map):
        memory_metrics = CADVISOR_METRICS['memory_metrics']
        for cadvisor_key, metric_name in memory_metrics.items():
            if cadvisor_key in memory_data:
                metric_value = memory_data[cadvisor_key]
                if self.report_container_metrics:
                    self._send_metrics("container." + metric_name, metric_value,
                                       container_dimensions,
                                       METRIC_TYPES_UNITS[metric_name][0],
                                       METRIC_TYPES_UNITS[metric_name][1])
                self._add_pod_metric(metric_name, metric_value, pod_key, pod_map)

    def _parse_filesystem(self, filesystem_data, container_dimensions):
        if not self.report_container_metrics:
            return
        filesystem_metrics = CADVISOR_METRICS['filesystem_metrics']
        for filesystem in filesystem_data:
            file_dimensions = container_dimensions.copy()
            file_dimensions['device'] = filesystem['device']
            for cadvisor_key, metric_name in filesystem_metrics.items():
                if cadvisor_key in filesystem:
                    self._send_metrics("container." + metric_name, filesystem[cadvisor_key], file_dimensions,
                                       METRIC_TYPES_UNITS[metric_name][0],
                                       METRIC_TYPES_UNITS[metric_name][1])

    def _parse_network(self, network_data, container_dimensions, pod_key, pod_net_metrics):
        network_interfaces = network_data['interfaces']
        network_metrics = CADVISOR_METRICS['network_metrics']
        for interface in network_interfaces:
            network_dimensions = container_dimensions.copy()
            network_interface = interface['name']
            network_dimensions['interface'] = network_interface
            for cadvisor_key, metric_name in network_metrics.items():
                if cadvisor_key in interface:
                    metric_value = interface[cadvisor_key]
                    if self.report_container_metrics:
                        self._send_metrics("container." + metric_name, metric_value, network_dimensions,
                                           METRIC_TYPES_UNITS[metric_name][0],
                                           METRIC_TYPES_UNITS[metric_name][1])
                    # Add metric to aggregated network metrics
                    if pod_key:
                        if pod_key not in pod_net_metrics:
                            pod_net_metrics[pod_key] = {}
                        if network_interface not in pod_net_metrics[pod_key]:
                            pod_net_metrics[pod_key][network_interface] = {}
                        if metric_name not in pod_net_metrics[pod_key][network_interface]:
                            pod_net_metrics[pod_key][network_interface][metric_name] = metric_value
                        else:
                            pod_net_metrics[pod_key][network_interface][metric_name] += metric_value

    def _parse_cpu(self, cpu_data, container_dimensions, pod_key, pod_metrics):
        cpu_metrics = CADVISOR_METRICS['cpu_metrics']
        cpu_usage = cpu_data['usage']
        for cadvisor_key, metric_name in cpu_metrics.items():
            if cadvisor_key in cpu_usage:
                # convert nanoseconds to seconds
                cpu_usage_sec = cpu_usage[cadvisor_key] / 1000000000
                if self.report_container_metrics:
                    self._send_metrics("container." + metric_name, cpu_usage_sec, container_dimensions,
                                       METRIC_TYPES_UNITS[metric_name][0],
                                       METRIC_TYPES_UNITS[metric_name][1])
                self._add_pod_metric(metric_name, cpu_usage_sec, pod_key, pod_metrics)

    def _add_pod_metric(self, metric_name, metric_value, pod_key, pod_metrics):
            if pod_key:
                if pod_key not in pod_metrics:
                    pod_metrics[pod_key] = {}
                if metric_name not in pod_metrics[pod_key]:
                    pod_metrics[pod_key][metric_name] = metric_value
                else:
                    pod_metrics[pod_key][metric_name] += metric_value

    def _get_container_dimensions(self, container, instance_dimensions, container_spec, container_dimension_map,
                                  pod_dimension_map):
        container_id = ""
        # meant to key through pod metrics/dimension dictionaries

        for alias in container_spec["aliases"]:
            if alias in container:
                container_id = alias
                break
        if container_id in container_dimension_map:
            container_dimensions = container_dimension_map[container_id]
            pod_key = container_dimensions['pod_name'] + container_dimensions['namespace']
            return pod_key, container_dimensions
        else:
            container_dimensions = instance_dimensions.copy()
            # Container image being used
            container_dimensions['image'] = container_spec['image']
            # First entry in aliases is container name
            container_dimensions['container_name'] = container_spec['aliases'][0]
            # check if container is a pause container running under a pod. Owns network namespace
            pod_key = None
            if 'labels' in container_spec:
                container_labels = container_spec['labels']
                if 'io.kubernetes.pod.namespace' in container_labels and 'io.kubernetes.pod.name' in container_labels:
                    pod_key = container_labels['io.kubernetes.pod.name'] + \
                        container_labels['io.kubernetes.pod.namespace']
                    # In case new pods showed up since we got our pod list from the kubelet
                    if pod_key in pod_dimension_map:
                        container_dimensions.update(pod_dimension_map[pod_key])
                        container_dimensions['container_name'] = container_labels['io.kubernetes.container.name']
                    else:
                        pod_key = None
            return pod_key, container_dimensions

    def _process_containers(self, cadvisor_url, dimensions, container_dimension_map, pod_dimension_map):
        try:
            cadvisor_spec_url = cadvisor_url + CADVISOR_SPEC_URL
            cadvisor_metric_url = cadvisor_url + CADVISOR_METRIC_URL
            containers_spec = self._get_result(cadvisor_spec_url)
            containers_metrics = self._get_result(cadvisor_metric_url)
        except Exception as e:
            self.log.error("Error getting data from cadvisor - {}".format(e))
            return
        # non-network pod metrics. Need by interface
        pod_metrics = {}
        # network pod metrics
        pod_network_metrics = {}
        for container, cadvisor_metrics in containers_metrics.items():
            pod_key, container_dimensions = self._get_container_dimensions(container,
                                                                           dimensions,
                                                                           containers_spec[container],
                                                                           container_dimension_map,
                                                                           pod_dimension_map)
            # Grab first set of metrics from return data
            cadvisor_metrics = cadvisor_metrics[0]
            if cadvisor_metrics['has_memory'] and cadvisor_metrics['memory']:
                self._parse_memory(cadvisor_metrics['memory'], container_dimensions, pod_key, pod_metrics)
            if cadvisor_metrics['has_filesystem'] and 'filesystem' in cadvisor_metrics \
                    and cadvisor_metrics['filesystem']:
                self._parse_filesystem(cadvisor_metrics['filesystem'], container_dimensions)
            if cadvisor_metrics['has_network'] and cadvisor_metrics['network']:
                self._parse_network(cadvisor_metrics['network'], container_dimensions, pod_key, pod_network_metrics)
            if cadvisor_metrics['has_cpu'] and cadvisor_metrics['cpu']:
                self._parse_cpu(cadvisor_metrics['cpu'], container_dimensions, pod_key, pod_metrics)
        self.send_pod_metrics(pod_metrics, pod_dimension_map)
        self.send_network_pod_metrics(pod_network_metrics, pod_dimension_map)

    def send_pod_metrics(self, pod_metrics_map, pod_dimension_map):
        for pod_key, pod_metrics in pod_metrics_map.items():
            pod_dimensions = pod_dimension_map[pod_key]
            for metric_name, metric_value in pod_metrics.items():
                self._send_metrics("pod." + metric_name, metric_value, pod_dimensions,
                                   METRIC_TYPES_UNITS[metric_name][0],
                                   METRIC_TYPES_UNITS[metric_name][1])

    def send_network_pod_metrics(self, pod_network_metrics, pod_dimension_map):
        for pod_key, network_interfaces in pod_network_metrics.items():
            pod_dimensions = pod_dimension_map[pod_key]
            for network_interface, metrics in network_interfaces.items():
                pod_network_dimensions = pod_dimensions.copy()
                pod_network_dimensions['interface'] = network_interface
                for metric_name, metric_value in metrics.items():
                    self._send_metrics("pod." + metric_name, metric_value, pod_network_dimensions,
                                       METRIC_TYPES_UNITS[metric_name][0],
                                       METRIC_TYPES_UNITS[metric_name][1])
