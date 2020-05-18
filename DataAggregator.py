import socket
import json
import configparser
import threading
import time
import queue
import re

#

class MyFiltersThread(threading.Thread):
    def __init__(self, queue, name, ttl, message_size, max_size, msg_count, aggregation_matric):
        super(MyFiltersThread, self).__init__()
        self.queue = queue
        self.name = name
        self.ttl = ttl
        if message_size == "None":
            self.message_size = None
        else:
            self.message_size = message_size
        if max_size == "None":
            self.max_size = None
        else:
            self.max_size = max_size
        if msg_count == "None":
            self.msg_count = None
        else:
            self.msg_count = msg_count
        self.messages = []
        self.aggregate_message = []
        self.aggregation_matric = aggregation_matric

    def run(self):
        #Checking the bucket size by TTL or Max total message size or number of messages inside the bucket
        #print(len(self.messages))

        if (time.sleep(self.ttl)) \
                or ((self.max_size != None and self.message_size != None) and ((self.max_size <= (len(self.messages))) * self.message_size)) \
                or ((self.msg_count != None) and (self.msg_count <= (len(self.messages)))):
        # if (time.sleep(self.ttl)):
            pass
        else:
            self.aggregate_message.append("count: " + str(len(self.messages)))
            self.aggregate_message.append("name: " + str(self.name))
            if len(self.messages) == 0:
                print("No messages to aggregate!")
                self.aggregate_message.append({})
            else:
                self.aggregate_message.append(self.messages[0])
                #print("Message after aggregation: " + str(self.messages))
            ##Firing the message!!!!!
            ##optional Can add last timestamp
            print("######################Message aggregation#####################\n: " + str(self.aggregate_message))

    def check_if_value_is_regex(self, value):
        try:
            re.compile(str(value))
            return True
        except re.error:
            return False

    def add_message(self):
        nested_matches = []
        normal_matches =[]
        print("The aggregation matric is: " + str(self.aggregation_matric))
        while not self.queue.empty():
            message = self.queue.get()

            #Check matric!!!!!! need to check nested + regex!!!
            for key, value in self.aggregation_matric.items():
                for msg_key, msg_val in message.items():
                    #print(type(msg_val))
                    #### Checking normal values
                    ##check if value is regular expression

                    if (key == msg_key) and isinstance(msg_val, dict) != True:
                        if (key == msg_key) and (value in msg_val):
                            #print(message)
                            #print(key + ":" + str(value))
                            normal_matches.append(msg_key + ":" + msg_val)
                            #print("Match!!")

                            pass
                        elif (key == msg_key) and self.check_if_value_is_regex(value) == True:
                            #check with value
                            m = re.search(value, msg_val)
                            match = m.group(0)
                            if (key == msg_key) and (match in msg_val):
                                # print(message)
                                # print(key + ":" + str(value))
                                normal_matches.append(msg_key + ":" + msg_val)
                                # print("Match!!")

                    elif isinstance(msg_val, dict):
                        if key == msg_key and isinstance(value, dict):
                            for value_key, value_val in value.items():
                                for nested_key, nested_value in msg_val.items():
                                    if self.check_if_value_is_regex(value_val):
                                        m = re.search(nested_value, nested_value)
                                        match = m.group(0)
                                        if (key == msg_key) and (value_key == nested_key) and (match in nested_value):
                                            # print(message)
                                            # print(key + ":" + str(value))
                                            nested_matches.append(key + ":" + str(value_key) + ":" + str(value_val))
                                            # print("Match!!")
                                        pass
                                    elif (key == msg_key) and (value_key == nested_key) and (value_val == nested_value):
                                        ###### Check if matric key val = nested key val
                                            #print(message)
                                            #print(key + ":" + str(value_key) + ":" + str(value_val))
                                            nested_matches.append(key + ":" + str(value_key) + ":" + str(value_val))
                                            #print("Nested Match!!")
                                            ####add validation matric length for savin loop times

                                    else:
                                        #print("Nested No match!")
                                        pass
                    else:
                        #print(message)
                        #print(key + ":" + str(value))
                        #print("NO Match!!")
                        pass
        if (self.aggregation_matric["nested_match_count"] == len(nested_matches)) and (self.aggregation_matric["normal_match_count"] == len(normal_matches)):
            print("Fount exact match!!!!!!!! for bucket name: " + self.name + " adding message to bucket...")
            print("Normal matches:" + str(normal_matches))
            print("Nested matches:" + str(nested_matches))
            self.messages.append(message)
#### create a clone option to recreate bucket in case it's stopped
    def clone(self):
        return MyFiltersThread(self.queue, self.name, self.ttl, self.message_size, self.max_size, self.msg_count, self.aggregation_matric)


filter_thread_list = []
def create_filter_bucket (name, ttl, message_size, max_size, msg_count, aggregation_matric):
    # Creating filter buckets
    q = queue.LifoQueue()
    thread = MyFiltersThread(q, name, ttl, message_size, max_size, msg_count, aggregation_matric=aggregation_matric)
    filter_thread_list.append(thread)
    thread.start()

# filter_thread_list = []
#
# #Creating filter buckets
# for i in range(4):
#     q = queue.LifoQueue()
#     thread = MyFiltersThread(q, i, 15, message_size=1024)
#     filter_thread_list.append(thread)
#     thread.start()


def add_message_to_thread_queue(thread_name, message):
    #print(thread.number, ": ", thread.is_alive())
    thread_name.queue.put(message)
    thread_name.add_message()

def check_backet_state(bucket_type):
    try:
        if bucket_type.is_alive():
            print("Bucket: " + bucket_type.getName() + " Is alive")
            return bucket_type
        else:
            filter_thread_list.remove(bucket_type)
            bucket_type = bucket_type.clone()
            bucket_type.start()
            filter_thread_list.append(bucket_type)
            print("Bucket: " + bucket_type.getName() + " restarted")
            return bucket_type
    except Exception as e:
        print(e)
        print("Issue with bucket: " + bucket_type.getName())


def check_threads_status():
    for thread in threading.enumerate():
        try:
            #print(thread.name, ": ", thread.is_alive())
            time.sleep(0.1)
        except Exception as e:
            print(e)
            print(thread.getName())

check_threads_status()
#Testing messages
# add_message_to_thread_queue(filter_thread_list[1], "first_message")
# time.sleep(5)
# for x in range(5):
#     add_message_to_thread_queue(filter_thread_list[1], "add from for loop " + str(x))
# print(filter_thread_list)
# if filter_thread_list[0].is_alive():
#     print(filter_thread_list[0].getName() + "is alive!")
# else:
#     print(filter_thread_list[0].getName() + "not alive!")
# time.sleep(20)
# if filter_thread_list[0].is_alive():
#     print(filter_thread_list[0].getName() + "is alive!")
# else:
#     print(filter_thread_list[0].getName() + "not alive!")
#
# print(filter_thread_list)
#check_threads_status()




### if you want to wait for comletion of threads
# print ("before join")
# check_threads_status()
# for thread in filter_thread_list:
#     thread.join()
# print ("This execute after all thread finished their jobs is added")
# print ("after join wait")
# check_threads_status()

#### Parsing aggregation map
# def get_filters_and_mapping():
#     config = configparser.ConfigParser()
#     config.read('aggregation_match_map')
#
#     feeds_types_to_aggregate = config.sections()
#
#     aggregation_dict = {}
#
#     aggregate_messages = []
#
#     for section in feeds_types_to_aggregate:
#         pre_key = None
#         for key in config[section]:
#             if 'lookupfield' in key:
#                 if '_' not in key:
#                     #print (config[section][key])
#                     #aggregatio_dict[section + '_' + 'look_up_fields'] = config[section][key]
#                     pre_key = key
#                 else:
#                     if key.split('_', 1)[0] == pre_key:
#                         agg_value = key.split('_', 1)[1]
#                         aggregation_dict[section + '_' + key] = config[section][key]
#                         pre_key = key
#                 aggregation_dict[section + '_' + key] = config[section][key]
#
#             else:
#                 if key == 'aggregationcount':
#                     aggregation_dict[section + '_' + key] = int(config[section][key])
#                 if key == 'aggregationbytes':
#                     aggregation_dict[section + '_' + key] = int(config[section][key])
#                 if key == 'ttl':
#                     aggregation_dict[section + '_' + key] = int(config[section][key])
#     return aggregation_dict

#aggregation_dict = get_filters_and_mapping()

with open('aggregation_mapping.json') as config_file:
    mapping_list = json.load(config_file)


#Creating the buckets!
for bucket in mapping_list:
    aggregation_matric = {}
    aggregation_matric["normal_match_count"] = 0
    aggregation_matric["nested_match_count"] = 0
    for key, value in bucket.items():
        if key == "ttl" or key == "messagescount" or key == "avg_message_in_bytes" or key == "max_message_size":
            continue
        else:
            aggregation_matric[key] = value
            ##### Counting the number of matches we need!
            if isinstance(value, dict):
                #for object in value.items():
                aggregation_matric["nested_match_count"] = aggregation_matric["nested_match_count"] + len(value)
            else:
                aggregation_matric["normal_match_count"] = aggregation_matric["normal_match_count"] + 1
    create_filter_bucket(bucket["type"], bucket["ttl"], bucket["avg_message_in_bytes"], bucket["max_message_size"], bucket["messagescount"], aggregation_matric=aggregation_matric)
    print(bucket)

check_threads_status()
print(filter_thread_list)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('localhost', 514))
s.listen(1)
conn, addr = s.accept()
event_msg_rcv_size = 10240000



#Start listen!!

while True:
    data = conn.recv(event_msg_rcv_size)
    #data.decode()
    data_in_utf_format = data.decode("utf-8")
    if not data:
        print ("No data")
        continue
    json_msg_type = json.loads(data_in_utf_format)
    for bucket_type in filter_thread_list:
        for feed_type in mapping_list:
            if bucket_type.getName() == feed_type["type"]:
                #Checking if bucket is alive
                # Print filter list
                print(filter_thread_list)
                bucket_type = check_backet_state(bucket_type)
                #Checking if message bilong to the bucket type
                if json_msg_type["type"] == feed_type["type"]:
                    add_message_to_thread_queue(bucket_type, json_msg_type)
                    print(json_msg_type)
                else:
                    #put message to drop ot fire
                    pass

#todo
# add config json validation
# Add regex support
# add int type support
# Add redis instead of using dict
# Create and check bucket size by TTL / Event size / Count
# High enthropy
# Check corolation and add to backet in case there is a match
# start filter thread if thread is not alive
# create drop backet for event drop






conn.close()