import sys
import argparse
import logging
import json
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
from datetime import datetime

log = logging.getLogger('WlanThermoGrafanaBridge')

def createSystemPoints(points, timestamp, message):
    log.debug('Createing system points from json payload')

    systemItem = {}
    systemItem['measurement'] = "System"
    systemItem['time'] = timestamp
    systemItem['fields'] = message['system']

    points.append(systemItem)

def createChannelPoints(points, timestamp, message):
    log.debug('Createing channel points from json payload')

    for channel in message['channel']:
        if channel['temp'] < 999: # 999 means channel not active
            channelItem = {}

            channelTags = {}
            channelTags['channel'] = "Channel {}".format(channel['number'])
            channelTags['alias'] = channel['name']

            channelItem['measurement'] = "Channel {}".format(channel['number'])
            channelItem['time'] = timestamp
            channelItem['tags'] = channelTags
            channelItem['fields'] = channel

            points.append(channelItem)

def createPitmasterPoints(points, timestamp, message):
    log.debug('Createing pitmaster points from json payload')

    pitmasterItem = {}
    pitmasterItem['measurement'] = "Pitmaster"
    pitmasterItem['time'] = timestamp
    pitmasterItem['fields'] = message['pitmaster']

    points.append(pitmasterItem)

def addPointsToDatabase(influxDbClient, points):
    if len(points) > 0:
        influxDbClient.write_points(points=points, database='wlanthermo')
        log.info('InfluxDB datapoints inserted')

def createDataPoints(message):
    points = []

    try:
        unixTimestamp = int(message['system']['time'])
        timestamp = None

        if unixTimestamp < 631152000: # 01.01.1990 00:00:00
            # internal system time of wlanthermo starts counting from 0 until external time is provided
            # we assume the system up time will never reach 631152000 on its own
            # if internal time is detected we use receive timestamps instead
            timestamp = datetime.utcnow()
            log.debug('Wlanthermo running on internal time > using receive timestamps')
        else:
            timestamp = datetime.fromtimestamp(unixTimestamp)

        timestampStr = timestamp.astimezone().replace(microsecond=0).isoformat()

        createSystemPoints(points, timestampStr, message)
        createChannelPoints(points, timestampStr, message)
        createPitmasterPoints(points, timestampStr, message)

        log.debug('Created points from json payload')
    except Exception as ex:
        log.warning('Failed to create points from json payload')
        log.debug(ex)

    return points

def mqtt_on_connect(client, influxDbClient, flags, rc):
    if rc == 0:
        log.info('Connection to mqtt broker successfull')
        client.subscribe('WLanThermo/+/status/settings')
        client.subscribe('WLanThermo/+/status/data')
        log.info('Subscribed to mqtt topics')
    else:
        log.error('Connection to mqtt broker failed rc={}'.format(rc))

def mqtt_on_message(client, influxDbClient, msg):
    decodedMessage = str(msg.payload.decode("utf-8","ignore"))
    log.info('Received unhandled mqtt message')
    log.debug(decodedMessage)

def on_wlanthermo_data(client, influxDbClient, msg):
    log.info('Received wlanthermo mqtt data message')
    decodedMessage = str(msg.payload.decode("utf-8","ignore"))
    log.debug(decodedMessage)
    messageJson = json.loads(decodedMessage)
    points = createDataPoints(messageJson)
    addPointsToDatabase(influxDbClient, points)

def on_wlanthermo_settings(client, influxDbClient, msg):
    decodedMessage = str(msg.payload.decode("utf-8","ignore"))
    log.info('Received wlanthermo mqtt settings message')
    log.debug(decodedMessage)
    # messageJson = json.loads(decodedMessage)
    # points = createSettingsPoints(messageJson)
    # addPointsToDatabase(influxDbClient, points)

def main(argv):
    mqtt_client_name = "wlanThermoGrafanaBridge"

    parser = argparse.ArgumentParser(description='WlanThermoGrafanaBridge')
    parser.add_argument('--mqttHost', metavar='<mqtt hostname>', type=str, default='localhost', help='MQTT host')
    parser.add_argument('--mqttPort', metavar='<mqtt port>', type=int, default=1883, help='MQTT port')
    parser.add_argument('--mqttUsername', metavar='<mqtt username>', type=str, default='', help='MQTT username')
    parser.add_argument('--mqttPassword', metavar='<mqtt password>', type=str, default='', help='MQTT password')
    parser.add_argument('--influxDbHost', metavar='<influxDB host>', type=str, default='localhost', help='InfluxDB host')
    parser.add_argument('--influxDbPort', metavar='<influxDB port>', type=int, default=8086, help='InfluxDB port')
    parser.add_argument('--influxDbUsername', metavar='<influxDB username>', type=str, default='', help='InfluxDB username')
    parser.add_argument('--influxDbPassword', metavar='<influxDB password>', type=str, default='', help='InfluxDB password')
    parser.add_argument('--influxDbName', metavar='<influxDB name>', type=str, default='wlanthermo', help='InfluxDB database name')
    parser.add_argument('--logLevel', metavar='<log level>', type=str, choices=['debug', 'info', 'warning', 'error'], default='info', help='Set Log level')
    parser.add_argument('--logFile', metavar='<log file filepath>', type=str, help='Logile output file')

    try:
        args = parser.parse_args()
    except Exception as ex:
        log.error('Failed to parse command line arguments')
        log.error(ex)
        exit(1)

    logLevels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR
    }

    logging.basicConfig(level=logLevels.get(args.logLevel, logging.INFO), filename=args.logFile)

    influxDbClient = InfluxDBClient(host=args.influxDbHost, port=args.influxDbPort,
        username=args.influxDbUsername, password=args.influxDbPassword, database=args.influxDbName)

    mqttClient = mqtt.Client(client_id=mqtt_client_name, clean_session=True, userdata=influxDbClient)
    mqttClient.username_pw_set(args.mqttUsername, args.mqttPassword)
    mqttClient.on_connect = mqtt_on_connect
    mqttClient.on_message = mqtt_on_message

    mqttClient.message_callback_add('WLanThermo/+/status/settings', on_wlanthermo_settings)
    mqttClient.message_callback_add('WLanThermo/+/status/data', on_wlanthermo_data)

    try:
        log.info('Connect to InfluxDB {}:{}'.format(args.influxDbHost, args.influxDbPort))
        influxDbClient.create_database('wlanthermo')
        log.info('Connect to InfluxDB successful')
    except Exception as ex:
        log.error('Connection to InfluxDB failed')
        log.error(ex)
        exit(1)

    try:
        log.info('Connect to mqtt broker {}:{}'.format(args.mqttHost, args.mqttPort))
        mqttClient.connect(args.mqttHost, args.mqttPort)
    except Exception as ex:
        log.error('Connection to mqtt broker failed')
        log.error(ex)
        exit(1)

    mqttClient.loop_forever()

if __name__ == "__main__":
    main(sys.argv[1:])