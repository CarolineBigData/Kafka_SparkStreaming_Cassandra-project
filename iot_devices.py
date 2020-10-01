#!/usr/bin/python3

# imports
from kafka import KafkaProducer # pip install kafka-python
import numpy as np              # pip install numpy
from sys import argv, exit
from time import time, sleep
from random import randint

# different device "profiles" with different 
# distributions of values to make things interesting
# tuple --> (mean, std.dev)
PAYMENT_PROFILES = {
	"payment_frequency": {'click'},
	"payment_method": {'click'},
	"first_payment_date": {'click'},
}

# check for arguments, exit if wrong
if len(argv) != 2 or argv[1] not in PAYMENT_PROFILES.keys():
	print("please provide a valid device name:")
	for key in PAYMENT_PROFILES.keys():
		 print(f"  * {key}")
	print(f"\nformat: {argv[0]} PAYMENT_PROFILE")
	exit(1)

profile_name = argv[1]
profile = PAYMENT_PROFILES[profile_name]

# set up the producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

count = 1

# until ^C
while True:
	# get random values within a normal distribution of the value
	click = randint(0, 3)

	
	# create CSV structure
	msg = f'{time()},{profile_name},{click}'

	# send to Kafka
	producer.send('portfolio_click', bytes(msg, encoding='utf8'))
	print(f'sending data to kafka, #{count}')

	count += 1
	sleep(.5)