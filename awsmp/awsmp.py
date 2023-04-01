"""Main module."""
import configparser
import os
import re
from concurrent.futures import as_completed  # isort:skip
from concurrent.futures import ProcessPoolExecutor  # isort:skip
from concurrent.futures import ThreadPoolExecutor  # isort:skip
from itertools import product


import boto3
import enlighten


class AwsMp:
    """
    AWS Multithreaded Processor
    """

    _showprogress = False
    _mp_use_processes = False
    _mp_workers = None  # use the default definded by python

    def __init__(
        self,
        use_processes=False,
        mp_workers=None,
        show_progress=False,
    ):
        self._showprogress = show_progress
        self._mp_workers = mp_workers
        self._mp_use_processes = use_processes

    def _get_all_regions(self, profile):
        result = []
        try:
            session = boto3.Session(profile_name=profile)
            ec2_client = session.client("ec2", region_name="us-east-1")
            region_list = ec2_client.describe_regions().get("Regions")
            result = [region.get("RegionName") for region in region_list]
        except Exception as ex:
            print(f"Error getting regions: {ex}")
        return result

    def _get_all_profiles(self):
        user_home = os.path.expanduser("~")
        config = configparser.ConfigParser()
        config.read(f"{user_home}/.aws/credentials")
        return config.sections()

    def _parse_region_param(self, profile, regions):
        _all_regions = self._get_all_regions(profile)
        region_list = None
        if isinstance(regions, str):
            regions = regions.lower().strip()
            if regions in ("none", ""):
                region_list = []
            else:
                region_list = list(regions)
        elif isinstance(regions, list):
            region_list = regions
        else:
            region_list = []
        region_list = [x.lower().strip() for x in region_list]
        not_a_region = [x for x in region_list if x not in _all_regions]
        if not_a_region:
            raise ValueError(f"Invalid region(s) passed: {not_a_region}")
        return region_list

    def _show_progress(self, futures):
        done_count = 0
        with enlighten.Manager as manager:
            with manager.counter(
                total=len(futures),
                color="green3",
                desc="Tasks",
                unit="tasks",
            ) as progress_bar:
                while done_count < len(futures):
                    done_count = 0
                    for future in futures:
                        if future.done():
                            done_count += 1
                    progress_bar.count = done_count
                    progress_bar.refresh()

    @property
    def showprogress(self):
        """return current showprogress setting"""
        return self._showprogress

    @showprogress.setter
    def showprogress(self, value: bool):
        """set progress bar visibility"""
        self._showprogress = value

    def awsmp(self, func, profile_filter=".*", regions=None):
        """Run function agains all profiles and regions"""
        results = []
        exceptions = []
        region_list = []

        try:
            profile_list = [
                x for x in self._get_all_profiles() if re.search(profile_filter, x)
            ]
        except Exception as ex:
            print(f"Error parsing profiles: {profile_filter}")
            print(f"{ex}")

        try:
            region_list = self._parse_region_param(profile_list[0], regions)
        except Exception as ex:
            print(ex)

        profile_region_list = [
            {"profile": item[0], "region": item[1]}
            for item in list(product(profile_list, region_list))
        ]

        if self._mp_use_processes:
            pool = ProcessPoolExecutor(max_workers=self._mp_workers)
        else:
            pool = ThreadPoolExecutor(max_workers=self._mp_workers)

        futures = [pool.submit(func, **kwarg) for kwarg in profile_region_list]
        pool.shutdown(wait=False)

        if self._showprogress:
            self._show_progress(futures)

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as ex:
                exceptions.append(ex)
            else:
                results.append(result)
        return (results, exceptions)
