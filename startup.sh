pip install --no-cache-dir -r requirements.txt;

nohup python analizeMQTTdata.py > output_analize.log &

python publisherMQTT.py;