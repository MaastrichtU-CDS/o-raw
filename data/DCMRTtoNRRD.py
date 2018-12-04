"""
###############################
@author: zhenwei.shi, Maastro##
###############################
Usage:

"""
from __future__ import print_function

import pydicom,os
import pandas as pd
import numpy as np
import re
import glob
import yaml
import shutil
from DicomDatabase import DicomDatabase
from subprocess import call
from pathlib import Path

def dcmRtToNRRD(inputRtDir,inputImageDir,exportDir,ROIname):
    try:
        call(['plastimatch', 'convert','--input',inputRtDir,'--output-prefix',exportDir, '--prefix-format', 'nrrd',\
        '--referenced-ct',inputImageDir])
    except:
        print("Error: plastimatch failed to convert RT to mask.nrrd")
    for label in os.listdir(exportDir):
        if not re.search(ROIname,label):
            os.remove(os.path.join(exportDir,label))