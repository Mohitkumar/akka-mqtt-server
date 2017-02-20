package io

import codec.{MQTT_3_1_1, MQTT_3_1, MqttVersion}

/**
  * Created by Mohit Kumar on 2/20/2017.
  */
object Util {

  def isValidClientId(mqttVersion: MqttVersion, clientId:String):Boolean = {
    if (mqttVersion == MQTT_3_1) {
      return clientId != null && clientId.length() >= Decoder.MIN_CLIENT_ID_LENGTH &&
        clientId.length() <= Decoder.MAX_CLIENT_ID_LENGTH;
    }
    if (mqttVersion == MQTT_3_1_1) {
      // In 3.1.3.1 Client Identifier of MQTT 3.1.1 specification, The Server MAY allow ClientId’s
      // that contain more than 23 encoded bytes. And, The Server MAY allow zero-length ClientId.
      return clientId != null;
    }
    throw new IllegalArgumentException(s" $mqttVersion is unknown mqtt version");
  }
}
