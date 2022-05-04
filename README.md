# DAGSched

In the input panel, please upload an input file (or one of the sample ones), then move to the second panel where you can select scheduling policies. After this is done simply click the run button and you will be able to view the scheduling results. With the control buttons (pause/play/stop/backwards/forwards) you can animate the state of the scheduler (this will also adjust the scheduling output like avg. job completion time and so forth).

## Input File

Below we have a sample input file (`./data/simple_dag.yml`):

```
# submit size of cluster
cluster:
  cpus: 20
  ram: 100

# submit user DAGs
users:
  test_user:
    name: Test User 1 # nice name to use
    arrival_time: 0 # integer, arrival time
    tasks:
      task_1:
        label: Task 1 # nice name to use in UI
        duration: 5
        cpus: 5
        ram: 10
      task_2:
        label: Task 2
        duration: 5
      task_3:
        label: Task 3
        duration: 5
        dependencies: [task_1, task_2] # specify dependencies
      task_4:
        label: Task 4
        duration: 3
        dependencies: [task_3]
  test_user2:
    name: Test User 2
    arrival_time: 0
    tasks:
      task_1:
        label: Task 1
        duration: 5
        cpus: 5
        ram: 10
      task_2:
        label: Task 2
        duration: 5
      task_3:
        label: Task 3
        duration: 5
        dependencies: [task_1, task_2]
      task_4:
        label: Task 4
        duration: 3
        dependencies: [task_3]
      task_5:
        label: Task 5
        duration: 3
        dependencies: [task_4]
```


# Development

Use black for automatically formatting files on save.

See [here](https://dev.to/adamlombard/how-to-use-the-black-python-code-formatter-in-vscode-3lo0) for an example of setting this up with VS Code.

```
# setup
pip3 install -r requirements.txt

# run main dash app
python3 app.py

# run unit tests
py.test
```
