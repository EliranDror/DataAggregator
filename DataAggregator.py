import socket
import json
import configparser
import threading
import time
import queue
#

class MyFiltersThread(threading.Thread):
    def __init__(self, queue, name, ttl, message_size, max_size=None):
        super(MyFiltersThread, self).__init__()
        self.queue = queue
        self.name = name
        self.ttl = ttl
        self.message_size = message_size
        self.max_size = max_size
        self.messages = []
    def run(self):
        #print("Running" + str(self.number))
        #time.sleep(self.ttl)
        if time.sleep(self.ttl) or (self.max_size != None and (self.max_size >= (len(self.messages))*self.message_size)):
            pass
        else:
            (self.messages.append("count: " + str(len(self.messages))))
            print(self.messages)

    def add_message(self):
        while not self.queue.empty():
            self.messages.append(self.queue.get())

            #print(self.messages)


    # def do_thing_with_message(self, message):
    #     if self.receive_messages:
    #         with print_lock:
    #             print(threading.currentThread().getName(), "Received {}".format(self.queue.get()))



filter_thread_list = []

#Creating filter buckets
for i in range(4):
    q = queue.LifoQueue()
    thread = MyFiltersThread(q, i, 15, message_size=1024)
    filter_thread_list.append(thread)
    thread.start()


def add_message_to_thread_queue(thread_name, message):
    #print(thread.number, ": ", thread.is_alive())
    thread_name.queue.put(message)
    thread_name.add_message()

def check_threads_status():
    for thread in threading.enumerate():
        try:
            print(thread.name, ": ", thread.is_alive())
            time.sleep(0.1)
        except:
            print(thread.getName())

check_threads_status()
#print(filter_thread_list)
add_message_to_thread_queue(filter_thread_list[1], "first_message")
time.sleep(5)
for x in range(5):
    add_message_to_thread_queue(filter_thread_list[1], "add from for loop " + str(x))

print(filter_thread_list)
if filter_thread_list[0].is_alive():
    print(filter_thread_list[0].getName() + "is alive!")
else:
    print(filter_thread_list[0].getName() + "not alive!")
time.sleep(20)
if filter_thread_list[0].is_alive():
    print(filter_thread_list[0].getName() + "is alive!")
else:
    print(filter_thread_list[0].getName() + "not alive!")

print(filter_thread_list)
#check_threads_status()




### if you want to wait for comletion of threads
# print ("before join")
# check_threads_status()
# for thread in filter_thread_list:
#     thread.join()
# print ("This execute after all thread finished their jobs is added")
# print ("after join wait")
# check_threads_status()




    # def add(self, item, ttl):
    #     set.add(self, item)
    #     t = threading.Thread(target=ttl_set_remove, args=(self, item, ttl))
    #     t.start()


# s = MySet()
# s.add("start_filter1", 20)
#
#
# s.add('start_filter2', 10)
# s.add('start_filter3', 2)
# s.add('start_filter4', 2)

# print(s)
# time.sleep(20)
# print(s)
#
# >>>
# MySet({'c', 'b', 'a'})
# MySet({'b', 'a'})


def check_corolation(feed_type, count, bytes):
    for key, values in aggregation_dict.items():
        if feed_type in key:
            print(key, values)

config = configparser.ConfigParser()
config.read('aggregation_match_map')

feeds_types_to_aggregate = config.sections()

aggregation_dict = {}

aggregate_messages = []

for section in feeds_types_to_aggregate:
    pre_key = None
    for key in config[section]:
        if 'lookupfield' in key:
            if '_' not in key:
                #print (config[section][key])
                #aggregatio_dict[section + '_' + 'look_up_fields'] = config[section][key]
                pre_key = key
            else:
                if key.split('_', 1)[0] == pre_key:
                    agg_value = key.split('_', 1)[1]
                    aggregation_dict[section + '_' + key] = config[section][key]
                    pre_key = key

            aggregation_dict[section + '_' + key] = config[section][key]

        else:
            if key == 'aggregationcount':
                aggregation_dict[section + '_' + key] = config[section][key]
            if key == 'aggregationbytes':
                aggregation_dict[section + '_' + key] = config[section][key]

    print(aggregation_dict)


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('localhost', 514))
s.listen(1)
conn, addr = s.accept()
event_msg_rcv_size = 10240000
aggregator_match_map = ''


#Start listen!!

while True:
    data = conn.recv(event_msg_rcv_size)
    #data.decode()
    data_in_utf_format = data.decode("utf-8")
    if not data:
        print ("No data")
        continue
    json_msg_type = json.loads(data_in_utf_format)
    for feed_type in feeds_types_to_aggregate:
        if json_msg_type['type'] == feed_type:
            ###Appanding message in order to check for match - this will must be Async function
            aggregate_messages.append(json_msg_type)

        #print(data)
        print(json_msg_type)


#todo
# Add redis instead of using dict
# Check corolation and add to backet in case there is a match
# start filter thread if thread is not alive
# check for





conn.close()