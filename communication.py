import functools
import threading
import time
import logging
import json
import os
import datetime
from json import JSONEncoder
import pika
from pika import exceptions
import requests
import re


class Container(object):
    def __init__(self, id, project, date_created, modules_completed_count, dicom_objects):
        self.id = id
        self.project = project
        self.date_created = date_created
        self.modules_completed_count = modules_completed_count
        self.dicom_objects = dicom_objects


class DicomObject(object):
    def __init__(self, sop_instance_uid, file_path, sop_class_uid):
        self.sop_instance_uid = sop_instance_uid
        self.file_path = file_path
        self.sop_class_uid = sop_class_uid


class ObjectEncoder(JSONEncoder):
    def default(self, o):
        for key in o.__dict__:
            new_key = re.sub("([_])([a-zA-Z])", lambda p: p.group(0).upper(), key)
            new_key = new_key.replace("_", "")
            o.__dict__[new_key] = o.__dict__.pop(key)
        return o.__dict__

class CommunicationService:
    def __init__(self, service_name, callback):
        self.service_name = service_name
        self.callback = callback

    @staticmethod
    def delete_files(dicom_objects):
        for dicom_object in dicom_objects:
            logging.debug('Deleting dicom file: %s', dicom_object)
            os.remove(dicom_object.file_path)

    @staticmethod
    def notify(container):
        headers = {'Content-type': 'application/json'}
        container_json = json.dumps(container, cls=ObjectEncoder)
        response = requests.post('http://orchestration-service:9000/api/container',
                                 container_json,
                                 headers=headers)
        logging.info('Notified orchestration service with container: %s', response)

    @staticmethod
    def get_files(dicom_objects):
        for dicom_object in dicom_objects:
            response = requests.get('http://dicom-service:8080/api/dicom?sopuid=' + dicom_object.sop_instance_uid)
            directory_path = os.getcwd() + '/processing/' + datetime.datetime.now().strftime('%Y%m%d%H%M%S') \
                             + dicom_object.sop_class_uid + '/'
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
            file_path = directory_path + dicom_object.sop_instance_uid + '.dcm'
            if os.path.exists(file_path):
                print("removing existing file for " + file_path)
                os.remove(file_path)
            file = open(file_path, 'wb')
            file.write(response.content)
            file.close()
            dicom_object.file_path = file_path

    @staticmethod
    def dicom_object_decoder(dicom_object):
        return DicomObject(dicom_object['sopInstanceUid'], dicom_object['filePath'], dicom_object['sopClassUid'])


    @staticmethod
    def container_decoder(container):
        return Container(container['id'], container['project'], container['dateCreated'],
                         container['modulesCompletedCount'], container['dicomObjects'])

    @staticmethod
    def object_decoder(obj):
        if 'sopInstanceUid' in obj:
            return CommunicationService.dicom_object_decoder(obj)
        else:
            return CommunicationService.container_decoder(obj)

    def ack_message(self, channel, delivery_tag):
        if channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            pass

    def consume_queue_item(self, connection, channel, delivery_tag, body):
        container = json.loads(body, object_hook=CommunicationService.object_decoder)
        logging.info('Created container: %s', container)
        CommunicationService.get_files(container.dicom_objects)
        try:
            self.callback(container.dicom_objects)
        except Exception as e:
            logging.error('Exception occurred during processing: %s' % e, exc_info=True)
        CommunicationService.delete_files(container.dicom_objects)
        CommunicationService.notify(container)
        cb = functools.partial(self.ack_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)

    def on_message(self, channel, method_frame, header_frame, body, args):
        (connection, threads) = args
        delivery_tag = method_frame.delivery_tag
        t = threading.Thread(target=self.consume_queue_item, args=(connection, channel, delivery_tag, body))
        t.start()
        threads.append(t)

    def listen_to_queue(self):
        connection = {}
        while True:
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host='consulrabbit'))
            except exceptions.AMQPConnectionError:
                logging.info('Unable to connect to RabbitMQ, retrying in 5 seconds.')
                time.sleep(5)
                continue
            break

        outPath = r''
        loggingFolder = os.path.join(outPath, 'log')
        if not os.path.exists(loggingFolder):
            os.makedirs(loggingFolder)
        progress_filename = os.path.join(loggingFolder, 'O-RAW_log.txt')
        # Configure logging
        rLogger = logging.getLogger('radiomics')

        # Set logging level
        # rLogger.setLevel(logging.INFO)  # Not needed, default log level of logger is INFO
        handler = logging.FileHandler(filename=progress_filename, mode='w')   # Create handler for writing to log file
        handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s: %(message)s'))
        rLogger.addHandler(handler)
        logger = rLogger.getChild('batch') # Initialize logging for batch log messages

        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel.queue_declare(queue=self.service_name, durable=True)
        threads = []
        on_message_callback = functools.partial(self.on_message, args=(connection, threads))
        channel.basic_consume(self.service_name, on_message_callback)
        logging.info('Listening to queue: %s', self.service_name)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

        # Wait for all to complete
        for thread in threads:
            thread.join()

        connection.close()
