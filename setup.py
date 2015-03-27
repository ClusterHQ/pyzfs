from setuptools import setup, find_packages

setup(
    name = "pyzfs",
    version = "0.1",
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    install_requires = [
        "cffi",
    ],
    setup_requires = [
        "cffi",
    ],
    zip_safe = False,
)

# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
