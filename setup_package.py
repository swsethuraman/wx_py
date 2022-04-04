"""
    cd into wx-py
    python setup_package.py bdist_wheel --dist-dir=S:\WX\Models\PROD\wx_wheels
"""
import setuptools
import subprocess
import os

cwd = os.path.dirname(os.path.abspath(__file__))
req__ = os.path.join(cwd, 'requirements.txt')
install_requires = open(req__, 'r', encoding='utf-8').readlines()


setuptools.setup(
    name="Wx",
    version="0.6.27",
    author="Team WX",
    author_email="swami.sethuraman@laurioncap.com",
    description="Wx derivatives pricing and portfolio management",
    url="",
    packages=setuptools.find_packages(),
    long_description="",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating SYstem :: OS Independent"
    ],
    python_requires='>=3.5'
    # install_requires=install_requires
)
