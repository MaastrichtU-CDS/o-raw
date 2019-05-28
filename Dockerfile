FROM python:3.6

#RUN apt-get update
# RUN wget https://bootstrap.pypa.io/get-pip.py
# RUN python get-pip.py

RUN apt-get update && apt-get install -y unzip python-dev

RUN pip install pandas SPARQLWrapper rdflib  pika requests pydevd-pycharm~=191.7479.10

#RUN wget https://github.com/Radiomics/pyradiomics/archive/2.1.0.zip && \
#    unzip 2.1.0.zip
RUN git clone https://github.com/Radiomics/pyradiomics.git

RUN cd /pyradiomics && \
    git checkout tags/2.1.0 && \
    python -m pip install -r requirements.txt && \
    python setup.py install

RUN mkdir /o-raw
RUN mkdir /data

ADD ./RadiomicsOntology/ /o-raw/RadiomicsOntology
ADD ./ParamsSettings/ /o-raw/ParamsSettings

COPY ./DicomDatabase.py /o-raw/DicomDatabase.py
COPY ./ORAW_Docker.py /o-raw/ORAW_Docker.py
COPY ./ORAW_DockerScript.py /o-raw/ORAW_DockerScript.py
COPY ./PyrexOutput.py /o-raw/PyrexOutput.py
COPY ./PyrexReader.py /o-raw/PyrexReader.py
COPY ./PyrexWithParams.py /o-raw/PyrexWithParams.py
COPY ./PyrexXNAT.py /o-raw/PyrexXNAT.py
COPY ./requirements.txt /o-raw/requirements.txt
COPY ./QueueListener.py /o-raw/QueueListener.py
COPY ./communication.py /o-raw/communication.py

RUN mv /o-raw/ /pyradiomics/o-raw/ && \
    cd /pyradiomics/o-raw/ && \
    python -m pip install -r requirements.txt

RUN echo "cd /pyradiomics/o-raw" >> /run.sh
RUN echo "python QueueListener.py" >> /run.sh
RUN chmod +x /run.sh

ENTRYPOINT "/run.sh" && /bin/bash
