from setuptools import setup, find_packages
import versioneer
from pathlib import Path

root_dir = Path(__file__).parent


setup(
    packages=find_packages(exclude=['test', 'test.*', 'test_*']),
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    platforms="linux"
)