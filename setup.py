# Copyright 2015 ClusterHQ. See LICENSE file for details.

from setuptools import setup, find_packages

setup(
    name = "pyzfs",
    version = "0.1",
    description = "Wrapper for libzfs_core",
    author = "ClusterHQ",
    author_email = "support@clusterhq.com",
    url = "https://clusterhq.com/",
    license = "Apache License, Version 2.0",

    packages = find_packages(),
    include_package_data = True,
    install_requires = [
        "cffi",
    ],
    setup_requires = [
        "cffi",
    ],
    zip_safe = False,
    test_suite = "libzfs_core.test",
)

# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
