import os
import time

import requests

payload = {"task2Name": "task_5"}

default_conductor_api = 'http://localhost:8080/api'
default_workflow_count = 50
default_expect_spawning_time_secs = 5
default_expect_completion_time_secs = 60

conductor_api = os.getenv('CONDUCTOR_API', default_conductor_api)
workflow_count = int(os.getenv('EXPECT_WORKFLOW_COUNT', default_workflow_count))
expect_spawning_time_secs = float(os.getenv('EXPECT_WORKFLOW_CREATION_TIME_SECS', default_expect_spawning_time_secs))
expect_completion_time_secs = float(os.getenv('EXPECT_WORKFLOW_COMPLETION_TIME_SECS', default_expect_completion_time_secs))
print("****************************************")
print("* conductor_api:[{}]  ".format(conductor_api))
print("* workflow_count:[{}] ".format(workflow_count))
print("* expect_spawning_time_secs:[{}] ".format(expect_spawning_time_secs))
print("* expect_completion_time_secs:[{}] ".format(expect_completion_time_secs))
print("****************************************")

def patch_es_task():
    elastic_search_task={
        "name": "search_elasticsearch",
        "taskReferenceName": "get_es_1",
        "inputParameters": {
            "http_request": {
                "uri": "http://preview-elasticsearch-client:9200/conductor/_search?size=10",
                "method": "GET"
            }
        },
        "type": "HTTP"
    }
    response = requests.put(
        url='{}/metadata/taskdefs'.format(conductor_api),
        json=elastic_search_task,
        headers={'content-type': 'application/json'}
    )
    if response.ok:
        print('patched task: {}'.format(elastic_search_task))

def count_running_worklow():
    res = requests.get('{0}/workflow/running/kitchensink?version=1'.format(conductor_api))
    return len(res.json())


def spawn():
    print("**** spawning workflow .... ****\n")
    for x in range(1, workflow_count):
        r = requests.post(
            url='{0}/workflow/kitchensink'.format(conductor_api),
            json=payload,
            headers={'content-type': 'application/json'}
        )
        print("{} -> {}".format(x, r.text))


start_time = time.time()

patch_es_task()
spawn()
time_to_spawn = time.time() - start_time
print(" - spawning time [{}]".format(time_to_spawn))
assert expect_spawning_time_secs > time_to_spawn, 'TIME TO CREATE WORK FLOWS MUST BE LOWER THAN [{}] secs'.format(
    expect_spawning_time_secs)

current_elapsed = time.time() - start_time

while ( count_running_worklow() > 0 ):
    time.sleep(1)
    current_elapsed = time.time() - start_time
    print("waiting until all workflow completed...")
    print("total time so far [{}]".format(current_elapsed))

assert expect_completion_time_secs > current_elapsed, 'TIME TO COMPLETE WORKLOWS MUST BE LOWER THAN [{}] secs'.format(
    expect_completion_time_secs)
