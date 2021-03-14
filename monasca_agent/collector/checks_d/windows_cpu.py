# (C) Copyright 2021 Bangmod Enterprise Co, Ltd
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import logging
import re
import subprocess

import monasca_agent.collector.checks as checks
from monasca_agent.common.psutil_wrapper import psutil


log = logging.getLogger(__name__)


class WindowsCpu(checks.AgentCheck):

    def __init__(self, name, init_config, agent_config):
        super(WindowsCpu, self).__init__(name, init_config, agent_config)

    def check(self, instance):
        num_of_metrics = 0
        dimensions = self._set_dimensions(None, instance)

        if instance is not None:
            send_rollup_stats = instance.get("send_rollup_stats", False)
        else:
            send_rollup_stats = False

        cpu_stats = psutil.cpu_times_percent(interval=None, percpu=False)
        cpu_times = psutil.cpu_times(percpu=False)
        cpu_perc = psutil.cpu_percent(interval=None, percpu=False)

        data = {'cpu.user_perc': cpu_stats.user,
                'cpu.system_perc': cpu_stats.system,
                'cpu.interrupt_perc': cpu_stats.interrupt,
                'cpu.idle_perc': cpu_stats.idle,
                'cpu.dpc_perc': cpu_stats.dpc,
                'cpu.percent': cpu_perc,
                'cpu.idle_time': cpu_times.idle,
                'cpu.interrupt_time': cpu_times.interrupt,
                'cpu.user_time': cpu_times.user,
                'cpu.system_time': cpu_times.system,
                'cpu.dpc_time': cpu_times.dpc}

        # Call lscpu command to get cpu frequency
        self._add_cpu_freq(data)

        for key, value in data.items():
            if data[key] is None or instance.get('cpu_idle_only') and 'idle_perc' not in key:
                continue
            self.gauge(key, value, dimensions)
            num_of_metrics += 1

        if send_rollup_stats:
            self.gauge('cpu.total_logical_cores', psutil.cpu_count(logical=True), dimensions)
            num_of_metrics += 1
        log.debug('Collected {0} cpu metrics'.format(num_of_metrics))

    def _add_cpu_freq(self, data):
        data['cpu.frequency_mhz'] = psutil.cpu_freq(percpu=False).max
