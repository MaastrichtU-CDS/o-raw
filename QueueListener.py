#!/usr/bin/env python
from __future__ import print_function

import pydicom
import requests
import ORAW_Docker
import glob
import shutil
import time
import os
import logging
import sys
# import pydevd_pycharm

from pydicom.tag import Tag
from communication import CommunicationService


def run_oraw(dicom_objects):
    print("Running o-raw")
    start_time1 = time.clock()
    roi = 'all'
    export_format = 'csv'
    export_name = 'o-raw'
    CTWorkingDir = "./CTFolder"
    STRUCTWorkingDir = "./StructFolder"

    if not os.path.exists(CTWorkingDir):
        os.makedirs(CTWorkingDir)
    if not os.path.exists(STRUCTWorkingDir):
        os.makedirs(STRUCTWorkingDir)

    patient_id = ''
    rtstruct = ''
    ct = []
    for dicom_object in dicom_objects:
        if dicom_object.sop_class_uid == '1.2.840.10008.5.1.4.1.1.481.3':
            rtstruct = dicom_object
            dcm_header = pydicom.dcmread(dicom_object.file_path)
            patient_id = dcm_header[Tag(0x10, 0x20)].value
        else:
            ct.append(dicom_object)
    if rtstruct == '' or len(ct) == 0:
        return
    baseUrl = "http://rdf-store:7200"
    if os.environ.get("RDF4J_URL") is not None:
        baseUrl = os.environ.get("RDF4J_URL")

    # Set default repo
    repo = "data"
    if os.environ.get("DBNAME") is not None:
        repo = os.environ.get("DBNAME")

    # Set default named graph
    localGraphName = "radiomics.local"
    if os.environ.get("NAMED_GRAPH") is not None:
        localGraphName = os.environ.get("NAMED_GRAPH")

    # Set interval between executions
    sleepTime = 60
    if os.environ.get("INTERVAL") is not None:
        sleepTime = int(os.environ.get("INTERVAL"))

    excludeStructRegex = "(.*)"
    if os.environ.get("EXCLUDE_STRUCTURE_REGEX") is not None:
        excludeStructRegex = os.environ.get("EXCLUDE_STRUCTURE_REGEX")

    includeStructRegex = "(^GTV.*)"
    if os.environ.get("INCLUDE_STRUCTURE_REGEX") is not None:
        includeStructRegex = os.environ.get("INCLUDE_STRUCTURE_REGEX")

    # ----------------------------------------------------
    if export_format == 'rdf':
        exportDir = './RFstore/Turtle_output'  # export format is RDF
    else:
        exportDir = './RFstore/CSV_output'  # export format is CSV
    if not os.path.exists(exportDir):
        os.makedirs(exportDir)
    # check if the temp CT/STRUCT folder is empty
    if not (os.listdir(CTWorkingDir) == [] and os.listdir(STRUCTWorkingDir) == []):
        ct_files = glob.glob(os.path.join(CTWorkingDir, '*'))
        for f in ct_files:
            os.remove(f)
        struct_files = glob.glob(os.path.join(STRUCTWorkingDir, '*'))
        for f in struct_files:
            os.remove(f)
    shutil.copy2(rtstruct.file_path, os.path.join(STRUCTWorkingDir, 'struct.dcm'))
    for i in range(len(ct)):
        shutil.copy2(ct[i].file_path, os.path.join(CTWorkingDir, str(i) + ".dcm"))
    time.clock()
    result = ORAW_Docker.executeORAWbatch_all([patient_id], roi, rtstruct.sop_instance_uid, exportDir, export_format,
                                             export_name, [CTWorkingDir], [STRUCTWorkingDir], excludeStructRegex,
                                             includeStructRegex)
    if export_format == 'rdf':
        turtle = result.serialize(format='nt')
        loadRequest = requests.post(baseUrl + "/repositories/" + repo + "/rdf-graphs/" + localGraphName,
                                    data=turtle,
                                    headers={
                                        "Content-Type": "text/turtle"
                                    }
                                    )
    else:
        logging.info('Extraction complete, writing CSV')
        outputFilepath = os.path.join(exportDir, export_name+'.csv')
        result.T.to_csv(outputFilepath, index=False, na_rep='NaN',mode='a+')
        logging.info('CSV writing complete')
        logging.info("Total Execution Time of O-RAW: %s seconds ---" % (time.clock() - start_time1))

sys.path.append("pydevd-pycharm.egg")

# docker_host_address = os.environ['DOCKER_HOST']
# print('docker host address: %s' % docker_host_address)
# pydevd_pycharm.settrace(docker_host_address, port=5059, stdoutToServer=True, stderrToServer=True)

communication_service = CommunicationService('o-raw', run_oraw)
communication_service.listen_to_queue()
