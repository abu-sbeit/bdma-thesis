class EventRecord:
    def __init__(self, kit_id, timestamp, resource, activity_name, quantity, processed, id = None, rack= None, washing_machine= None, sterilization_machine = None):
        self._id = id
        self._kit_id = kit_id
        self._timestamp = timestamp
        self._resource = resource
        self._activity_name = activity_name
        self._quantity = quantity
        self._processed = processed
        self._rack = rack
        self._washing_machine = washing_machine
        self._sterilization_machine = sterilization_machine

    def __dict__(self):
        return {
            "id": self._id,
            "kit_id": self._kit_id,
            "timestamp": self._timestamp,
            "resource": self._resource,
            "activity_name": self._activity_name,
            "quantity": self._quantity,
            "processed": self._processed,
            "washing_machine": self._washing_machine,
            "sterilization_machine": self._sterilization_machine
        }
    
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def kit_id(self):
        return self._kit_id

    @kit_id.setter
    def kit_id(self, value):
        self._kit_id = value

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        self._timestamp = value

    @property
    def resource(self):
        return self._resource

    @resource.setter
    def resource(self, value):
        self._resource = value

    @property
    def activity_name(self):
        return self._activity_name

    @activity_name.setter
    def activity_name(self, value):
        self._activity_name = value

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        self._quantity = value

    @property
    def processed(self):
        return self._processed

    @processed.setter
    def processed(self, value):
        self._processed = value

    @property
    def rack(self):
        return self._rack

    @rack.setter
    def rack(self, value):
        self._rack = value

    @property
    def washing_machine(self):
        return self._washing_machine

    @washing_machine.setter
    def washing_machine(self, value):
        self._washing_machine = value

    @property
    def sterilization_machine(self):
        return self._sterilization_machine

    @sterilization_machine.setter
    def sterilization_machine(self, value):
        self._sterilization_machine = value

class TaskExecution:
    def __init__(self, task, data):
        self._task = task
        self._data = data

    def __dict__(self):
        return {
            "task": self._task,
            "data": self._data
        }
    
    @property
    def task(self):
        return self._task

    @task.setter
    def task(self, value):
        self._task = value

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        self._data = value
        