#!/usr/bin/env python
from __future__ import print_function

import ORAW_Docker
import glob
import shutil
import time
import os

from communication import CommunicationService


def run_oraw(dicom_objects):
    print("Running o-raw")
    start_time1 = time.clock()
    roi = 'all'
    export_format = 'rdf'
    export_name = 'ORAW_Dockertest-1'
    CTWorkingDir = "./CTFolder"
    STRUCTWorkingDir = "./StructFolder"

    if not os.path.exists(CTWorkingDir):
        os.makedirs(CTWorkingDir)
    if not os.path.exists(STRUCTWorkingDir):
        os.makedirs(STRUCTWorkingDir)

    rtstruct = ''
    ct = []
    for dicom_object in dicom_objects:
        if dicom_object.sop_class_uid == '1.2.840.10008.5.1.4.1.1.481.3':
            rtstruct = dicom_object
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
    # check if the temp CT/STRUCT folder is empty
    if not (os.listdir(CTWorkingDir) == [] and os.listdir(STRUCTWorkingDir) == []):
        ct_files = glob.glob(os.path.join(CTWorkingDir, '*'))
        for f in ct_files:
            os.remove(f)
        struct_files = glob.glob(os.path.join(STRUCTWorkingDir, '*'))
        for f in struct_files:
            os.remove(f)
    # copy RTSTRUCT file to tmp folder as 'struct.dcm'
    shutil.copy2(rtstruct.file_path, os.path.join(STRUCTWorkingDir, 'struct.dcm'))
    for i in range(len(ct)):
        shutil.copy2(ct[i].file_path, os.path.join(CTWorkingDir, str(i) + ".dcm"))
    start_time = time.clock()
    graph = ORAW_Docker.executeORAWbatch_all(['interobs5'], roi, rtstruct.sop_instance_uid, exportDir, export_format,
                                             export_name, [CTWorkingDir], [STRUCTWorkingDir], excludeStructRegex,
                                             includeStructRegex)
    print("Call O-RAW_Docker - time %s seconds ---" % (time.clock() - start_time))
    #####################
    # Load RDF store with new data
    #####################
    # get string of turtle triples (nt format)
    # turtle = graph.serialize(format='nt')
    #
    # # upload to RDF store
    # loadRequest = requests.post(baseUrl + "/repositories/" + repo + "/rdf-graphs/" + localGraphName,
    #                             data=turtle,
    #                             headers={
    #                                 "Content-Type": "text/turtle"
    #                             }
    #                             )
    print("Total Execution Time of O-RAW: %s seconds ---" % (time.clock() - start_time1))


communication_service = CommunicationService('o-raw', run_oraw)
communication_service.listen_to_queue()
