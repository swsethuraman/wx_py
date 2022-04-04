import pkg_resources
import re
import wx
import os
import subprocess

wx_version = pkg_resources.get_distribution("wx").version

wx_match = []
rgx = re.compile(r'Wx.*\.whl$')
files = [f for f in os.listdir('.') if os.path.isfile(f)]
for f in files:
    if rgx.match(f):
        wx_match.append(f)
wx_latest = max(wx_match, key=os.path.getctime)
wx_latest_version = wx_latest.split('-')[1]


def lex_cmp(s1):
    s1 = s1.split('.')
    n = len(s1)
    s1_value = [int(s1[i])*(10**(3*(n-i-1))) for i in range(n)]
    return sum(s1_value)


wx_max = max(wx_latest_version, wx_version, key=lex_cmp)

try:
    if wx_max != wx_version:

        subprocess.call(["pip", "install", wx_latest])
        print("Successful installation of the latest Wx version!")
    else:
        print("Latest Wx version already installed.")
except Exception as e:
    print("Failed: Could not install the latest version of Wx")


if __name__ == '__main__':
    print(wx_version)
    print(wx_latest)
    print(wx_latest_version)
    print(wx_latest_version > wx_version)
