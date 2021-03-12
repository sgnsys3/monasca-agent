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
import os
import psutil
import re
import platform

log = logging.getLogger(__name__)

import monasca_agent.collector.checks as checks


class Disk(checks.AgentCheck):

    def __init__(self, name, init_config, agent_config):
        self._partition_error = set()
        super(Disk, self).__init__(name, init_config, agent_config)

    def _log_once_per_day(self, message):
        if message in self._partition_error:
            return
        self._partition_error.add(message)
        log.exception(message)

    def check(self, instance):
        """Capture disk stats

        """

        dimensions = self._set_dimensions(None, instance)
        rollup_dimensions = dimensions.copy()

        if instance is not None:
            use_mount = instance.get("use_mount", True)
            send_io_stats = instance.get("send_io_stats", True)
            send_rollup_stats = instance.get("send_rollup_stats", False)
            # If we filter devices, get the list.
            device_blacklist_re = self._get_re_exclusions(instance)
            fs_types_to_ignore = self._get_fs_exclusions(instance)
        else:
            use_mount = True
            send_io_stats = True
            send_rollup_stats = False
            device_blacklist_re = None
            fs_types_to_ignore = set()

        partitions = psutil.disk_partitions(all=True)

        disk_count = 0
        total_capacity = 0
        total_used = 0

        for partition in partitions:
            if partition.fstype not in fs_types_to_ignore \
                and (not device_blacklist_re or
                     not device_blacklist_re.match(partition.device)):
                try:
                    mountpoint = self._get_mountpoint(partition)
                    device_name = self._get_device_name(partition)

                    disk_usage = psutil.disk_usage(mountpoint)
                    total_capacity += disk_usage.total
                    total_used += disk_usage.used
                except Exception as ex:
                    exception_name = ex.__class__.__name__
                    self._log_once_per_day('Unable to access partition {} '
                                           'with error: {}'.format(partition,
                                                                   exception_name))
                    continue

                if use_mount:
                    dimensions.update({'mount_point': mountpoint})
                self.gauge("disk.space_used_perc",
                    disk_usage.percent,
                    device_name=device_name,
                    dimensions=dimensions)
                disk_count += 1

                log.debug('Collected {0} disk usage metrics for partition {1}'.format(
                    disk_count,
                    mountpoint))
                disk_count = 0
        
        if send_io_stats:
            disk_stats = psutil.disk_io_counters(perdisk=True)
            for device_name in disk_stats:
                stats = disk_stats[device_name]
                self.rate("io.read_req_sec", round(float(stats.read_count), 2),
                            device_name=device_name, dimensions=dimensions)
                self.rate("io.write_req_sec", round(float(stats.write_count), 2),
                            device_name=device_name, dimensions=dimensions)
                self.rate("io.read_kbytes_sec",
                            round(float(stats.read_bytes / 1024), 2),
                            device_name=device_name, dimensions=dimensions)
                self.rate("io.write_kbytes_sec",
                            round(float(stats.write_bytes / 1024), 2),
                            device_name=device_name, dimensions=dimensions)
                self.rate("io.read_time_sec", round(float(stats.read_time / 1000), 2),
                            device_name=device_name, dimensions=dimensions)
                self.rate("io.write_time_sec", round(float(stats.write_time / 1000), 2),
                            device_name=device_name, dimensions=dimensions)

        if send_rollup_stats:
            self.gauge("disk.total_space_mb",
                       total_capacity / 1048576,
                       dimensions=rollup_dimensions)
            self.gauge("disk.total_used_space_mb",
                       total_used / 1048576,
                       dimensions=rollup_dimensions)
            log.debug('Collected 2 rolled-up disk usage metrics')

    def _get_re_exclusions(self, instance):
        """Parse device blacklist regular expression"""
        filter = None
        try:
            filter_device_re = instance.get('device_blacklist_re', None)
            if filter_device_re:
                filter = re.compile(filter_device_re)
        except re.error:
            log.error('Error processing regular expression {0}'.format(filter_device_re))

        return filter

    def _get_fs_exclusions(self, instance):
        """parse comma separated file system types to ignore list"""
        file_system_list = set()

        # automatically ignore filesystems not backed by a device
        try:
            for nodevfs in filter(lambda x: x.startswith('nodev\t'), open('/proc/filesystems')):
                file_system_list.add(nodevfs.partition('\t')[2].strip())
        except IOError:
            log.debug('Failed reading /proc/filesystems')

        try:
            file_systems = instance.get('ignore_filesystem_types', None)
            if file_systems:
                # Parse file system types
                file_system_list.update(x.strip() for x in file_systems.split(','))
        except ValueError:
            log.info("Unable to process ignore_filesystem_types.")

        return file_system_list

    def _get_device_name(self, partition):
        device_name = mountpoint = re.sub("\\\\", "/", partition.device)
        return device_name

    def _get_mountpoint(self, partition):
        mountpoint = re.sub("\\\\", "/", partition.mountpoint)
        return mountpoint