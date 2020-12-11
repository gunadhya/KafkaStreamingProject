"""Contains functionality related to Weather"""
import logging
import json

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
#         logger.info("weather process_message is incomplete - skipping")
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
#         print("topic",message.topic())
#         print("message_value",message.value())
#         value = message.value()
#         self.temperature = value["temperature"]
#         self.status = value["status"]
#         logger.debug("weather : %sf | %s",self.temperature,self.status.replace("_",""))

#         try:
#             value = json.loads(str(message.value()))
#             print("hello")
#             print(value)
#             logger.info('%s',value)
#             self.temperature = value.get('temperature')
#             self.status = value.get('status')
#         except Exception as e:
#             logger.error(e)

#         print("topic", message.topic())
#         print("message_value", message.value())
        logger.info(f"handle weather : {message.value()}")
        
        try:
            value = json.loads(message.value())
            print("value_temp", value['temperature'])
            self.temperature = value['temperature']
            self.status = value['status']
        except Exception as e:
            logger.fatal(f"Weather issues: {value} {e}")