"""VPE - Acquisition setup for installing package locally."""

from setuptools import find_packages, setup


def use_readme() -> str:
  """Use `README.md` for parsing long description."""
  with open('README.md') as file:
    return file.read()


with open('requirements.txt', 'r') as requirements:
  required_packages = [package.rstrip() for package in requirements]

setup(
  name="acquisition",
  version="2.0.0",
  url="https://github.com/Inpointtech/acquisition/",
  author="XA",
  author_email="akshay@inpointtech.com",
  maintainer="XA",
  maintainer_email="akshay@inpointtech.com",
  classifiers=[
    'Intended Audience :: Developers',
    'Intended Audience :: End Users/Desktop',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'Natural Language :: English',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3 :: Only',
    'Topic :: Multimedia',
    'Topic :: Multimedia :: Graphics :: Capture :: Digital Camera',
    'Topic :: Multimedia :: Sound/Audio',
    'Topic :: Multimedia :: Sound/Audio :: Players',
    'Topic :: Multimedia :: Video :: Capture',
    'Topic :: Scientific/Engineering',
    'Topic :: Scientific/Engineering :: Artificial Intelligence',
    'Topic :: Scientific/Engineering :: Image Recognition',
    'Topic :: Scientific/Engineering :: Information Analysis',
    'Topic :: Scientific/Engineering :: Mathematics',
    'Topic :: Software Development',
    'Topic :: Software Development :: Documentation',
    'Topic :: Software Development :: Libraries :: Python Modules',
    'Topic :: System :: Monitoring',
    'Topic :: System :: Networking :: Monitoring :: Hardware Watchdog',
    'Topic :: System :: Networking :: Time Synchronization',
  ],
  license="MIT",
  description=__doc__,
  long_description=use_readme(),
  long_description_content_type='text/markdown',
  keywords='vpe machine learning artificial intelligence pandas numpy cv2',
  zip_safe=False,
  install_requires=required_packages,
  python_requires='~=3.6',
  include_package_data=True,
  packages=find_packages(),
)
