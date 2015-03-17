
#
# Based on https://caremad.io/2014/11/distributing-a-cffi-project/
# XXX License?
#

from distutils.command.build import build
from setuptools import setup
from setuptools.command.install import install


def get_ext_modules():
	import example
	return [example.ffi.verifier.get_extension()]


class CFFIBuild(build):
	def finalize_options(self):
		self.distribution.ext_modules = get_ext_modules()
		build.finalize_options(self)


class CFFIInstall(install):
	def finalize_options(self):
		self.distribution.ext_modules = get_ext_modules()
		install.finalize_options(self)


setup(
	name = "pyzfs",
	version = "0.1",
	py_modules = ["libzfs_core"],
	install_requires = [
		"cffi",
	],
	setup_requires = [
		"cffi",
	],
	cmdclass = {
		"build": CFFIBuild,
		"install": CFFIInstall,
	},
	zip_safe = False,
)
