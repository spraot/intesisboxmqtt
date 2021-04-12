import asyncio
import os
import logging
import json
import yaml
import paho.mqtt.client as mqtt
import queue
import sys
import time
import atexit
from asyncio import ensure_future

_LOGGER = logging.getLogger('pyintesisbox')

API_DISCONNECTED = "Disconnected"
API_CONNECTING = "Connecting"
API_AUTHENTICATED = "Connected"

POWER_ON = 'ON'
POWER_OFF = 'OFF'
POWER_STATES = [POWER_ON, POWER_OFF]

MODE_AUTO = 'AUTO'
MODE_DRY = 'DRY'
MODE_FAN = 'FAN'
MODE_COOL = 'COOL'
MODE_HEAT = 'HEAT'
MODES = [MODE_AUTO, MODE_DRY, MODE_FAN, MODE_COOL, MODE_HEAT]

MODE_DRY_HA = 'fan_only'

FUNCTION_ONOFF = 'ONOFF'
FUNCTION_MODE = 'MODE'
FUNCTION_SETPOINT = 'SETPTEMP'
FUNCTION_FANSP = 'FANSP'
FUNCTION_VANEUD = 'VANEUD'
FUNCTION_VANELR = 'VANELR'
FUNCTION_AMBTEMP = 'AMBTEMP'
FUNCTION_ERRSTATUS = 'ERRSTATUS'
FUNCTION_ERRCODE = 'ERRCODE'

NULL_VALUE = '32768'


class IntesisBox(asyncio.Protocol):
    name = 'AC'
    id = 'ac'
    config_file = 'config.yml'
    topic_prefix = 'pi'
    homeassistant_prefix = 'homeassistant'
    mqtt_server_ip = "localhost"
    mqtt_server_port = 1883
    mqtt_server_user = ""
    mqtt_server_password = ""
    intesisbox_ip = ""
    intesisbox_port = 3310
    unique_id_suffix = '_intesisbox'
    unique_id = None

    def __init__(self):
        self._mac = None
        self._device = {}
        self._connectionStatus = API_DISCONNECTED
        self._commandQueue = queue.Queue()
        self._transport = None
        self._model: str = None
        self._firmversion: str = None
        self._rssi: int = None

        # Limits
        self._operation_list = []
        self._fan_speed_list = []
        self._vertical_vane_list = []
        self._horizontal_vane_list = []
        self._setpoint_minimum = None
        self._setpoint_maximum = None

        logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info("Init")

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()

        self.mqtt_config_topic = '{}/climate/{}/config'.format(self.homeassistant_prefix, self.unique_id)
        self.mqtt_state_topic = '{}/{}'.format(self.topic_prefix, self.id)
        self.availability_topic = '{}/{}/availability'.format(self.topic_prefix, self.id)
        self.mqtt_command_topic = '{}/{}/set'.format(self.topic_prefix, self.id)
        self.mqtt_power_command_topic = '{}/{}/power/set'.format(self.topic_prefix, self.id)
        self.mqtt_mode_command_topic = '{}/{}/mode/set'.format(self.topic_prefix, self.id)
        self.mqtt_temp_command_topic = '{}/{}/temp/set'.format(self.topic_prefix, self.id)

        #MQTT init
        self.mqttclient = mqtt.Client()
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.on_message = self.mqtt_on_message

         #Register program end event
        atexit.register(self.programend)

        logging.info("init done")

    def load_config(self):
        logging.info("Reading config from "+self.config_file)

        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)

        for key in ['name', 'id', 'topic_prefix', 'homeassistant_prefix', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'unique_id', 'intesisbox_ip', 'intesisbox_port']:
            try:
                self.__setattr__(key, config[key])
            except KeyError:
                pass

        if self.unique_id is None:
            self.unique_id = self.intesisbox_ip.replace('.', '_')+self.unique_id_suffix

        if id is None:
            self.id = self.unique_id

    def configure_mqtt(self):
        config = {
            'name': self.name,
            'power_command_topic': self.mqtt_power_command_topic,
            'mode_command_topic': self.mqtt_mode_command_topic,
            'temperature_command_topic': self.mqtt_temp_command_topic,
            'json_attributes_topic': self.mqtt_state_topic,
            'mode_state_topic': self.mqtt_state_topic,
            'mode_state_template': '{{ value_json.mode }}',
            'temperature_state_topic': self.mqtt_state_topic,
            'temperature_state_template': '{{ value_json.temperature }}',
            'current_temperature_topic': self.mqtt_state_topic,
            'current_temperature_template': '{{ value_json.current_temperature }}',
            'temperature_unit': 'C',
            'temp_step': 0.1,
            'initial': 22.5,
            "availability": [
                {'topic': self.availability_topic},
            ],
            "device": {
                "identifiers": [self.unique_id],
                "manufacturer": "Intesis",
                "model": self.device_model,
                "name": self.name,
                "sw_version": self._firmversion
            },
            'min_temp': self.min_setpoint,
            'max_temp': self.max_setpoint,
            'modes': [x.lower() if x != MODE_FAN else 'fan_only' for x in self._operation_list],
            'fan_modes': ['auto', 'low', 'medium', 'high'],
            "unique_id": self.unique_id
        }

        try:
            config['name'] = self.name
        except KeyError:
            config['name'] = self.unique_id

        config['device']['name'] = config["name"]

        json_conf = json.dumps(config)
        logging.debug("Broadcasting homeassistant configuration for unit: " + self.name + ":" + json_conf)
        self.mqttclient.publish(self.mqtt_config_topic, payload=json_conf, qos=0, retain=True)

    async def run(self):
        logging.info("starting")

        #MQTT startup
        logging.info("Starting MQTT client")
        self.mqttclient.username_pw_set(self.mqtt_server_user, password=self.mqtt_server_password)
        self.mqttclient.connect(self.mqtt_server_ip, self.mqtt_server_port, 60)
        self.mqttclient.loop_start()
        logging.info("MQTT client started")

        logging.info("Connecting to IntesisBox")
        await asyncio.wait_for(self.connect_to_intesisbox(), timeout=10)

        logging.info("started")

        while True:
            try:
                await asyncio.sleep(0.1)
            except KeyboardInterrupt:
                break

    def programend(self):
        logging.info("stopping")

        self.mqttclient.publish(self.availability_topic, payload="offline", qos=0, retain=True)

        self.mqttclient.disconnect()
        time.sleep(0.5)
        logging.info("stopped")

    def connection_made(self, transport):
        """asyncio callback for a successful connection."""
        _LOGGER.debug("Connected to IntesisBox")
        self._transport = transport

        self._transport.write("ID\r".encode('ascii'))
        self._transport.write("LIMITS:*\r".encode('ascii'))
        self._transport.write("GET,1:*\r".encode('ascii'))
        
        self.update_status()

    def mqtt_on_connect(self, client, userdata, flags, rc):
        logging.info("MQTT client connected with result code "+str(rc))

        #Subsribe to MQTT switch updates
        self.mqttclient.subscribe(self.mqtt_power_command_topic)
        self.mqttclient.subscribe(self.mqtt_mode_command_topic)
        self.mqttclient.subscribe(self.mqtt_temp_command_topic)
        self.mqttclient.subscribe(self.mqtt_command_topic)

        self.update_status()

    def mqtt_on_message(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode('utf-8').strip()
            logging.info("Received MQTT message on topic: " + msg.topic + ", payload: " + payload_str + ", retained: " + str(msg.retain))
            
            payload = {}
            if msg.topic == self.mqtt_power_command_topic:
                payload['state'] = payload_str

            if msg.topic == self.mqtt_mode_command_topic:
                payload['mode'] = payload_str

            if msg.topic == self.mqtt_temp_command_topic:
                payload['temperature'] = payload_str

            if msg.topic == self.mqtt_command_topic:
                payload = json.loads(payload_str)

            if 'state' in payload:
                if payload['state'].upper() == POWER_ON:
                    self.set_power_on()
                elif payload['state'].upper() == POWER_OFF:
                    self.set_power_off()
                    return
                else:
                    _LOGGER.error('Invalid state {}'.format(payload['state']))

            if 'mode' in payload:
                mode = MODE_DRY if payload['mode'].lower() == MODE_DRY_HA else payload['mode'].upper()
                if mode in self.operation_list:
                    self.set_mode(mode)
                else:
                    _LOGGER.error('Unsupported mode {}'.format(payload['mode']))

            if 'temperature' in payload:
                try:
                    self.set_temperature(float(payload['temperature']))
                except ValueError:
                    _LOGGER.error('Invalid temperature {}'.format(payload['temperature']))

            if 'vane_horizontal' in payload:
                mode = payload['vane_horizontal'].upper()
                if mode in self._horizontal_vane_list:
                    self.set_horizontal_vane(mode)
                else:
                    _LOGGER.error('Unsupported value for vane_horizontal {}'.format(payload['vane_horizontal']))

            if 'vane_vertical' in payload:
                mode = payload['vane_vertical'].upper()
                if mode in self._vertical_vane_list:
                    self.set_vertical_vane(mode)
                else:
                    _LOGGER.error('Unsupported value for vane_vertical {}'.format(payload['vane_vertical']))

            if 'fan_speed' in payload:
                mode = payload['fan_speed'].upper()
                fan_speeds_numeric = sorted([x for x in self._fan_speed_list if x.isnumeric()], key=lambda x: float(x))
                if mode == 'AUTO':
                    self.set_fan_speed(mode)
                elif len(fan_speeds_numeric) == 0:
                    pass
                elif mode == 'LOW':
                    self.set_fan_speed(fan_speeds_numeric[0])
                elif mode == 'MEDIUM':
                    self.set_fan_speed(fan_speeds_numeric[len(fan_speeds_numeric) // 2])
                elif mode == 'HIGH':
                    self.set_fan_speed(fan_speeds_numeric[-1])
                else:
                    _LOGGER.error('Unsupported value for fan_speed {}'.format(payload['fan_speed']))
        except Exception as e:
            logging.error('Encountered error: '+str(e))

    def mqtt_broadcast_state(self):
        state = {
            'state': 'on' if self.is_on else 'off',
            'temperature': self.setpoint,
            'current_temperature': self.ambient_temperature,
            'signal_strength': self.rssi
        }

        try:
            state['mode'] = self.mode.lower() if self.mode != MODE_FAN else 'fan_only'
        except AttributeError:
            state['mode'] = None

        try:
            fan_speeds_numeric = sorted([x for x in self._fan_speed_list if x.isnumeric()], key=lambda x: float(x))
            if self.fan_speed == 'AUTO':
                state['fan_speed'] = 'auto'
            elif self.fan_speed.isnumeric():
                mid = len(fan_speeds_numeric) // 2
                if fan_speeds_numeric.index(self.fan_speed) < mid:
                    state['fan_speed'] = 'low'
                elif fan_speeds_numeric.index(self.fan_speed) == mid:
                    state['fan_speed'] = 'medium'
                elif fan_speeds_numeric.index(self.fan_speed) > mid:
                    state['fan_speed'] = 'high'
            else:
                state['fan_speed'] = self.fan_speed.lower()
        except (AttributeError, ValueError):
            state['fan_speed'] = None

        try:
            state['vane_horizontal'] = self.horizontal_swing().lower()
        except AttributeError:
            state['vane_horizontal'] = None

        try:
            state['vane_vertical'] = self.vertical_swing().lower()
        except AttributeError:
            state['vane_vertical'] = None

        state = json.dumps(state)
        logging.debug("Broadcasting MQTT message on topic: " + self.mqtt_state_topic + ", value: " + state)
        self.mqttclient.publish(self.mqtt_state_topic, payload=state, qos=0, retain=True)

    def data_received(self, data):
        """asyncio callback when data is received on the socket"""
        decoded_data = data.decode('ascii')
        _LOGGER.debug("Data received: {}".format(decoded_data))
        linesReceived = decoded_data.splitlines()
        for line in linesReceived:
            cmdList = line.split(':', 1)
            cmd = cmdList[0]
            args = None
            if len(cmdList) > 1:
                args = cmdList[1]
                if cmd == 'ID':
                    self._parse_id_received(args)
                    self._connectionStatus = API_AUTHENTICATED
                elif cmd == 'CHN,1':
                    self._parse_change_received(args)
                elif cmd == 'LIMITS':
                    self._parse_limits_received(args)

    def _parse_id_received(self, args):
        # ID:Model,MAC,IP,Protocol,Version,RSSI
        info = args.split(',')
        if len(info) >= 6:
            self._model = info[0]
            self._mac = info[1]
            self._firmversion = info[4]
            self._rssi = info[5]
    
    def _parse_change_received(self, args):
        function = args.split(',')[0]
        value = args.split(',')[1]
        if value == NULL_VALUE:
            value = None
        changed = function not in self._device or self._device[function] != value
        self._device[function] = value

        if changed:
            self.mqtt_broadcast_state()

    def _parse_limits_received(self, args):
        split_args = args.split(',', 1)

        def limits():
            return [self._setpoint_minimum, self._setpoint_maximum, self._fan_speed_list, self._operation_list, self._vertical_vane_list, self._horizontal_vane_list]

        if len(split_args) == 2:
            function = split_args[0]
            values = split_args[1][1:-1].split(',')
            original_values = limits()

            if function == FUNCTION_SETPOINT and len(values) == 2:
                self._setpoint_minimum = int(values[0])/10
                self._setpoint_maximum = int(values[1])/10
            elif function == FUNCTION_FANSP:
                self._fan_speed_list = values
            elif function == FUNCTION_MODE:
                self._operation_list = values
            elif function == FUNCTION_VANEUD:
                self._vertical_vane_list = values
            elif function == FUNCTION_VANELR:
                self._horizontal_vane_list = values
            else:
                return

            if limits() != original_values:
                self.update_status()
            
    def update_status(self):
        if self.is_connected and self.mqttclient.is_connected:
            if self.is_connected:
                #Configure MQTT for Home Assistant
                self.configure_mqtt()

            self.mqttclient.publish(self.availability_topic, payload="online" if self.is_connected else "offline", qos=0, retain=True)
            
    async def connection_lost(self, exc):
        """asyncio callback for a lost TCP connection"""
        self._connectionStatus = API_DISCONNECTED
        _LOGGER.info('The server closed the connection')
        self.update_status()

        await asyncio.sleep(30)
        self.connect_to_intesisbox()

    def connect_to_intesisbox(self):
        """Public method for connecting to IntesisHome API"""
        if self._connectionStatus == API_DISCONNECTED:
            self._connectionStatus = API_CONNECTING
            try:
                # Must poll to get the authentication token
                if self.intesisbox_ip and self.intesisbox_port:
                    # Create asyncio socket
                    coro = asyncio.get_event_loop().create_connection(lambda: self, 
                                                             self.intesisbox_ip,
                                                             self.intesisbox_port)
                    _LOGGER.debug('Opening connection to IntesisBox %s:%s',
                                  self.intesisbox_ip, self.intesisbox_port)
                    return ensure_future(coro)
                else:
                    _LOGGER.debug("Missing IP address or port.")

            except Exception as e:
                _LOGGER.error('%s Exception. %s / %s', type(e), repr(e.args), e)
                self._connectionStatus = API_DISCONNECTED

    def disconnect_from_intesisbox(self):
        """Public method for shutting down connectivity with the envisalink."""
        self._connectionStatus = API_DISCONNECTED
        self._transport.close()

    def poll_status(self, sendcallback=False):
        self._transport.write("GET,1:*\r".encode('ascii'))

    def set_temperature(self, setpoint):
        """Public method for setting the temperature"""
        set_temp = int(setpoint * 10)
        self._set_value(FUNCTION_SETPOINT, set_temp)

    def set_fan_speed(self, fan_speed):
        """Public method to set the fan speed"""
        self._set_value(FUNCTION_FANSP, fan_speed)

    def set_vertical_vane(self, vane: str):
        """Public method to set the vertical vane"""
        self._set_value(FUNCTION_VANEUD, vane)

    def set_horizontal_vane(self, vane: str):
        """Public method to set the horizontal vane"""
        self._set_value(FUNCTION_VANELR, vane)

    def _set_value(self, uid, value):
        """Internal method to send a command to the API"""
        message = "SET,{}:{},{}\r".format(1, uid, value)
        try:
            self._transport.write(message.encode('ascii'))
            _LOGGER.debug("Data sent: {!r}".format(message))
        except Exception as e:
            _LOGGER.error('%s Exception. %s / %s', type(e), e.args, e)

    def set_mode(self, mode):
        if not self.is_on:
            self.set_power_on()

        if mode in MODES:
            self._set_value(FUNCTION_MODE, mode)
        
    def set_mode_dry(self):
        """Public method to set device to dry asynchronously."""
        self._set_value(FUNCTION_MODE, MODE_DRY)

    def set_power_off(self):
        """Public method to turn off the device asynchronously."""
        self._set_value(FUNCTION_ONOFF, POWER_OFF)

    def set_power_on(self):
        """Public method to turn on the device asynchronously."""
        self._set_value(FUNCTION_ONOFF, POWER_ON)

    @property
    def operation_list(self):
        return self._operation_list

    @property
    def vane_horizontal_list(self):
        return self._horizontal_vane_list
        
    @property
    def vane_vertical_list(self):
        return self._vertical_vane_list

    @property
    def mode(self) -> str:
        """Public method returns the current mode of operation."""
        return self._device.get(FUNCTION_MODE)

    @property
    def fan_speed(self) -> str:
        """Public method returns the current fan speed."""
        return self._device.get(FUNCTION_FANSP)

    @property
    def fan_speed_list(self):
        return self._fan_speed_list

    @property
    def device_mac_address(self) -> str:
        return self._mac

    @property
    def device_model(self) -> str:
        return self._model

    @property
    def is_on(self) -> bool:
        """Return true if the controlled device is turned on"""
        return self._device.get(FUNCTION_ONOFF) == POWER_ON

    @property
    def has_swing_control(self) -> bool:
        """Return true if the device supports swing modes."""
        return len(self._horizontal_vane_list) > 1 or len(self._vertical_vane_list) > 1

    @property
    def setpoint(self) -> float:
        """Public method returns the target temperature."""
        setpoint = self._device.get(FUNCTION_SETPOINT)
        if setpoint:
            setpoint = int(setpoint) / 10
        return setpoint

    @property
    def ambient_temperature(self) -> float:
        """Public method returns the current temperature."""
        temperature = self._device.get(FUNCTION_AMBTEMP)
        if temperature:
            temperature = int(temperature) / 10
        return temperature

    @property
    def max_setpoint(self) -> float:
        """Public method returns the current maximum target temperature."""
        return self._setpoint_maximum

    @property
    def min_setpoint(self) -> float:
        """Public method returns the current minimum target temperature."""
        return self._setpoint_minimum

    @property
    def rssi(self) -> str:
        """Public method returns the current wireless signal strength."""
        return self._rssi

    def vertical_swing(self) -> str:
        """Public method returns the current vertical vane setting."""
        return self._device.get(FUNCTION_VANEUD)

    def horizontal_swing(self) -> str:
        """Public method returns the current horizontal vane setting."""
        return self._device.get(FUNCTION_VANELR)

    @property
    def is_connected(self) -> bool:
        """Returns true if the TCP connection is established."""
        return self._connectionStatus == API_AUTHENTICATED

    @property
    def is_disconnected(self) -> bool:
        """Returns true when the TCP connection is disconnected and idle."""
        return self._connectionStatus == API_DISCONNECTED

    async def keep_alive(self):
        """Send a keepalive command to reset it's watchdog timer."""
        _LOGGER.info("keep_alive called")
        await asyncio.sleep(10)


if __name__ == "__main__":
    async def main():
        ib = IntesisBox()
        await ib.run()

    asyncio.run(main())