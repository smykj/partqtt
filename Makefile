CC = clang
mqttserver:
	$(CC) -std=c99 -Werror=pedantic mqtt.c -o mqttserver
