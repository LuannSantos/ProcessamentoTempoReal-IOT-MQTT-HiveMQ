# De dentro da pasta do projeto

echo "$1" | sudo -S mkdir /opt/Pipeline_StreamSets;
echo "$1" | sudo -S cp *.sh *.txt *.py  /opt/Pipeline_StreamSets;
echo "$1" | sudo -S cp -R registrar_log /opt/Pipeline_StreamSets;

echo "$1" | sudo -S mkdir /opt/Pipeline_StreamSets/logs /opt/Pipeline_StreamSets/data /opt/Pipeline_StreamSets/output;
echo "$1" | unzip data/archive.zip;
echo "$1" | rm -rf data/archive.zip;
echo "$1" | rm -rf data.json;
echo "$1" | sudo -S mv data.csv /opt/Pipeline_StreamSets/data/;

echo "$1" | sudo -S chown -R $USER:$USER /opt/Pipeline_StreamSets;
echo "$1" | sudo -S chmod +x /opt/Pipeline_StreamSets/analizeMQTTdata.py /opt/Pipeline_StreamSets/startup.sh /opt/Pipeline_StreamSets/publisherMQTT.py;
echo "$1" | sudo -S chmod +r /opt/Pipeline_StreamSets/requirements.txt;