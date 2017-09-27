# To parse the json Log into control messages and others
import json

LAST_STAGE_COMPLETE_TIME = -1
MS_TO_NS = 1000000
DRIVER = 0


class Executor:
    # def __init__(self, jlog):
    #   self.executorId = jlog["Executor ID"]
    #        self.cores = jlog["Executor Info"]["Total Cores"]

    def __init__(self, executor_id, core_num):
        self.executorId = executor_id
        self.cores = core_num


class CompletedStage:
    ## handle taskCompletionEvents
    def __init__(self, jlog):
        info = jlog["Stage Info"]
        self.id = int(info["Stage ID"])

        self.submission_ts = float(info["Submission Time"]) * MS_TO_NS  # ms -> ns
        print "minSubmission:{}".format(self.submission_ts)

        self.completion_ts = float(info["Completion Time"]) * MS_TO_NS  # ms -> ns
        print "minSubmission:{}".format(self.completion_ts)

        self.num_tasks = int(info["Number of Tasks"])
        self.broadcast_start_ts = float(jlog["BroadcastStartsTime"]) * MS_TO_NS
        self.broadcast_end_ts = float(jlog["BroadcastEndsTime"]) * MS_TO_NS
        self.destory_broadcast_start_ts = float(jlog["DestroyBroadcastStartsTime"]) * MS_TO_NS
        self.destory_broadcast_end_ts = float(jlog["DestroyBroadcastEndsTime"]) * MS_TO_NS
        self.update_weight_start_ts = float(jlog["updateWeightOnDriverStartsTime"]) * MS_TO_NS
        self.update_weight_end_ts = float(jlog["updateWeightOnDriverEndsTime"]) * MS_TO_NS
        self.judge_converge_start_ts = float(jlog["JudgeConvergeStartsTime"]) * MS_TO_NS
        self.judge_converge_end_ts = float(jlog["JudgeConvergeEndsTime"]) * MS_TO_NS

        self.last_completion_ts = self.submission_ts
        global LAST_STAGE_COMPLETE_TIME
        if LAST_STAGE_COMPLETE_TIME > 0:
            self.last_completion_ts = LAST_STAGE_COMPLETE_TIME
        LAST_STAGE_COMPLETE_TIME = self.completion_ts

        self.parents = list()  # store the parent stageIds
        for parent in jlog["Stage Info"]["Parent IDs"]:
            self.parents.append(int(parent))


class ProcessedStage:
    # stages that have been processed, have start_time and end_time aligned with those on driver.
    def __init__(self, completed_stage):
        # somethings needed here.
        self.id = completed_stage.id
        self.submission_ts = completed_stage.submission_ts
        self.completion_ts = completed_stage.completion_ts
        self.parents = completed_stage.parents

        self.workers = dict()  # which thread has been involved in the computation of this stage
        self.schedule = list()  # stores ScheduledTask

        self.num_tasks = completed_stage.num_tasks

        self.broadcast_start_ts = completed_stage.broadcast_start_ts
        self.broadcast_end_ts = completed_stage.broadcast_end_ts
        self.destory_broadcast_start_ts = completed_stage.destory_broadcast_start_ts
        self.destory_broadcast_end_ts = completed_stage.destory_broadcast_end_ts
        self.update_weight_start_ts = completed_stage.update_weight_start_ts
        self.update_weight_end_ts = completed_stage.update_weight_end_ts
        self.judge_converge_start_ts = completed_stage.judge_converge_start_ts
        self.judge_converge_end_ts = completed_stage.judge_converge_end_ts

        self.last_completion_ts = completed_stage.last_completion_ts
        self.parents = completed_stage.parents


class SubmittedStage:
    def __init__(self, jlog):
        info = jlog["Stage Info"]
        self.id = int(info["Stage ID"])
        self.num_tasks = int(info["Number of Tasks"])
        self.parents = list()  # store the parent stageIds

        for parent in info["Parent IDs"]:
            self.parents.append(int(parent))


class Task:
    def __init__(self, jlog):
        assert jlog["Event"] == "SparkListenerTaskEnd"

        # log on driver
        taskinfo = jlog["Task Info"]
        metrics = jlog["Task Metrics"]
        self.driver_send_rpc_ts = float(jlog["driverLanuchTime"]) * MS_TO_NS

        self.stageId = int(jlog["Stage ID"])
        self.executorId = int(taskinfo["Executor ID"])
        self.launch_time_ts = float(taskinfo["Launch Time"]) * MS_TO_NS
        self.finish_time_ts = float(taskinfo["Finish Time"]) * MS_TO_NS
        self.getting_result_time_ts = float(taskinfo["Getting Result Time"]) * MS_TO_NS

        self.shuffle_write = float(metrics["Shuffle Write Metrics"]["Shuffle Write Time"])  # already nanoseconds
        self.shuffle_read = float(metrics["Shuffle Read Metrics"]["Fetch Wait Time"]) * MS_TO_NS
        self.jvm_gc_time = float(metrics["JVM GC Time"]) * MS_TO_NS

        self.result_serialize = float(metrics["Result Serialization Time"]) * MS_TO_NS
        self.executor_deserialize = float(metrics["Executor Deserialize Time"]) * MS_TO_NS
        self.executor_run_time = float(metrics["Executor Run Time"]) * MS_TO_NS

        # log on executor
        self.task_decode_taskdesc_start_ts = float(jlog["DecodeStartsTime"]) * MS_TO_NS
        self.task_decode_taskdesc_end_ts = float(jlog["DecodeTaskDescEndsTime"]) * MS_TO_NS
        self.task_executor_lanuch_start_ts = float(jlog["ExecutorLanuchTaskTime"]) * MS_TO_NS
        self.task_deserialize_start_ts = float(jlog["taskDeserializeStarts"]) * MS_TO_NS
        self.task_deserialize_end_ts = float(jlog["taskDeserializeEnds"]) * MS_TO_NS
        self.task_run_start_ts = float(jlog["taskRunStarts"]) * MS_TO_NS

        self.executor_deserize_start_ts = float(jlog["ExecutorDeserialStarts"]) * MS_TO_NS
        self.executor_deserize_end_ts = float(jlog["ExecutorDeserialEnds"]) * MS_TO_NS

        self.task_run_end_ts = float(jlog["taskRunEnds"]) * MS_TO_NS
        self.task_result_serialize_start_ts = float(jlog["taskResultSerialStarts"]) * MS_TO_NS
        self.task_result_serialize_end_ts = float(jlog["taskResultSerialEnds"]) * MS_TO_NS
        self.task_result_serialize2_start_ts = float(jlog["taskResultSerial2Starts"]) * MS_TO_NS
        self.task_result_serialize2_end_ts = float(jlog["taskResultSerial2Ends"]) * MS_TO_NS
        self.task_put_result_into_local_blockmanager_start_ts = float(
            jlog["taskPutResultIntoLocalBlockMangerStarts"]) * MS_TO_NS
        self.task_put_result_into_local_blockmanager_end_ts = float(
            jlog["taskPutResultIntoLocalBlockMangerEnds"]) * MS_TO_NS

        self.input_map_start_ts = float(jlog["logInputMapStart"]) * MS_TO_NS
        self.input_map_end_ts = float(jlog["logInputMapEnd"]) * MS_TO_NS
        self.sample_filter_start_ts = float(jlog["logSampleFilterStart"]) * MS_TO_NS
        self.sample_filter_end_ts = float(jlog["logSampleFilterEnd"]) * MS_TO_NS
        self.seq_op_start_ts = float(jlog["logSeqOpStart"]) * MS_TO_NS
        self.seq_op_end_ts = float(jlog["logSeqOpEnd"]) * MS_TO_NS
        self.map_partition_with_index_start_ts = float(jlog["logMapPartitionWithIndexStart"]) * MS_TO_NS
        self.map_partition_with_index_end_ts = float(jlog["logMapPartitionWithIndexEnd"]) * MS_TO_NS
        self.com_op_start_ts = float(jlog["logCombOpStart"]) * MS_TO_NS
        self.com_op_end_ts = float(jlog["logCombOpEnd"]) * MS_TO_NS

    def deserialize_executor_duration(self):
        return self.executor_deserize_end_ts - self.executor_deserize_start_ts

    def task_runner_duration(self):
        return self.task_result_serialize_start_ts - self.executor_deserize_start_ts

    def executor_duration(self):
        # time for running in the executor
        # include decode the task description, taskRunner time, serialize results,
        # and putResultIntoLocalBlockManger
        return self.task_put_result_into_local_blockmanager_end_ts - self.task_decode_taskdesc_start_ts


    def computing_duration(self):
        return self.executor_duration() - self.shuffle_read - self.shuffle_write - self.deserialize_executor_duration()


    def get_result_duration(self):
        if self.getting_result_time_ts == 0:
            return 0
        else:
            assert self.finish_time_ts > self.getting_result_time_ts
            return self.finish_time_ts - self.getting_result_time_ts


    def total_duration(self):
        # include scheduler delay, executor_duration, driver getting result
        return self.finish_time_ts - self.launch_time_ts


    def scheduler_delay(self):
        res = self.total_duration() - self.executor_duration() - self.get_result_duration()
        assert res > 0
        return res


class ScheduledTask:
    # tasks whose time are rescheduled according to the driver
    def __init__(self, task, start_time, end_time, worker_id):
        self.task = task
        self.start_time = start_time
        self.end_time = end_time
        self.worker_id = worker_id


class Thread:
    def __init__(self, worker_id, index, busy_until):
        self.worker_id = worker_id
        self.index = index
        self.busy_until = busy_until


class SparkState:
    def __init__(self, delay_split, default_cores):
        self.executors = dict()  # executorId --> Executor
        self.threads = dict()  # executorId --> list[Thread]
        self.submitted = dict()  # stageId --> SubmittedStage, stages just submitted, not all tasks are seen
        self.completed = dict()  # stageId --> CompletedStage
        self.processed = dict()  # stageId --> ProcessedStage
        self.skipped = dict()  # stageId --> -1. record skipped stages

        self.task_queue = list()  # a list of tasks
        self.prev_worker_id = 0  # DRIVER is 0
        self.executor_core_num = default_cores
        self.delay_split = delay_split

    def generate_worker_id(self):  # logically it is thread Id
        self.prev_worker_id += 1
        return self.prev_worker_id

    def add_executor(self, executor):
        assert not executor.executorId in self.executors
        # note that the number of cores should have been set in the executors
        # but here we use the one specified here
        # the executor should have a executorId
        executor_id = executor.executorId
        self.executors[executor_id] = executor

        threads_list = list()
        for i in range(self.executor_core_num):
            threads_list.append(Thread(self.generate_worker_id(), i, 0))
        # what is the index used for?
        # the workerId here is a global one, say we have 10 machines,
        # each with 8 cores, then the workerId ranges from 1 to 80, Driver is 0.

        self.threads[executor_id] = threads_list

    def submit_stage(self, submitted_stage):
        stage_id = submitted_stage.id
        assert stage_id not in self.submitted
        assert stage_id not in self.skipped

        for parent in submitted_stage.parents:
            if parent not in self.processed:
                self.skipped[parent] = -1

    def complete_stage(self, completed_stage):
        stage_id = completed_stage.id
        # assert stage_id in self.submitted
        # assert stage_id not in self.completed
        # assert stage_id not in self.skipped

        submitted_stage = self.submitted.pop(stage_id, None)  # remove the stage from submitted
        self.completed[stage_id] = completed_stage

        if len(self.submitted) == 0:
            self.schedule_tasks()

    def schedule_tasks(self):
        # schedule all the tasks and stages, align the time
        task_num = 0
        for stage in self.completed:
            task_num += self.completed[stage].num_tasks

        assert len(self.task_queue) == task_num

        # sort the task by lanuch_ts, note that this time is one driver.
        self.task_queue.sort(key=lambda x: x.launch_time_ts)  # sort in place

        for stage_id in self.completed:
            stage = self.completed[stage_id]
            # assert stage_id not in self.processed
            self.processed[stage_id] = ProcessedStage(stage)

        processed_stages = self.processed  # processed stages, constructed from completed stages

        for task in self.task_queue:
            stage = processed_stages[task.stageId]
            shipping_delay = self.delay_split * task.scheduler_delay()
            task_duration = task.executor_duration()
            start_time = task.launch_time_ts + shipping_delay
            end_time = start_time + task_duration  # the time when getting result starts

            available_threads = filter(lambda x: x.busy_until < start_time, self.threads[task.executorId])

            # why we want to use the most recently one? This is not correct but a assumption.
            # because two cores have no strong connections, since they are logical concepts
            to_use_thread = max(lambda x: x.busy_until, available_threads)
            to_use_thread_x = to_use_thread[0]
            to_use_thread_x.busy_until = end_time
            stage.workers[to_use_thread_x.worker_id] = -1  # mean this thread is in use now.

            scheduled_task = ScheduledTask(task, start_time, end_time, to_use_thread_x.worker_id)
            stage.schedule.append(scheduled_task)

        self.processed.update(processed_stages)

    def add_task(self, task):
        # check whether we need to add a new executor
        if task.executorId not in self.executors:
            executor = Executor(task.executorId, self.executor_core_num)
            self.add_executor(executor)

        self.task_queue.append(task)

    def read_json(self, inf):
        for line in open(inf):
            jlog = json.loads(line)
            eventType = jlog["Event"]
            if eventType == "SparkListenerExecutorAdded":
                executorId = int(jlog["Executor ID"])
                core_nums = int(jlog["Executor Info"]["Total Cores"])

                self.add_executor(Executor(executorId, core_nums))
            elif eventType == "SparkListenerTaskEnd":
                self.add_task(Task(jlog))
            elif eventType == "SparkListenerStageSubmitted":
                self.submit_stage(SubmittedStage(jlog))
            elif eventType == "SparkListenerStageCompleted":
                self.complete_stage(CompletedStage(jlog))
            else:
                pass

    def write_logs(self):
        for stageId in self.processed:
            stage = self.processed[stageId]
            op = stage.id

            if stage.broadcast_start_ts > 0:
                write_duration(DRIVER, stage.broadcast_start_ts,
                               stage.broadcast_end_ts - stage.broadcast_start_ts, "Broadcast", op)
            if stage.destory_broadcast_start_ts > 0:
                write_duration(DRIVER, stage.destory_broadcast_start_ts,
                               stage.destory_broadcast_end_ts - stage.destory_broadcast_start_ts, "DestoryBroadcast",
                               op)
            if stage.update_weight_start_ts > 0:
                write_duration(DRIVER, stage.update_weight_start_ts,
                               stage.update_weight_end_ts - stage.update_weight_start_ts, "UpdateWeight", op)

            if stage.judge_converge_start_ts > 0:
                write_duration(DRIVER, stage.judge_converge_start_ts,
                               stage.judge_converge_end_ts - stage.judge_converge_start_ts, "JudgeConverge", op)

            for scheduled in stage.schedule:
                task = scheduled.task
                worker = scheduled.worker_id
                send_ts = task.lanuch_ts
                recv_ts
                scheduled.start_time

                local_start = task.task_deserialize_start_ts
                write_control(DRIVER, send_ts, worker, recv_ts)

                if task.task_decode_taskdesc_end_ts - task.task_decode_taskdesc_start_ts > 0:
                    write_duration(worker, task.task_decode_taskdesc_start_ts + recv_ts - local_start,
                                   task.task_decode_taskdesc_end_ts - task.task_decode_taskdesc_start_ts,
                                   "DecodeTaskDesc", op)

                # task deserize and executor deserize
                write_duration(worker, task.task_deserialize_start_ts + recv_ts - local_start,
                               task.executor_deserize_end_ts - task.task_deserialize_start_ts,
                               "Deserialization", op)

                start_read = task.executor_deserize_end_ts + recv_ts - local_start
                if task.shuffle_read > 0:
                    write_duration(worker, start_read, task.shuffle_read,
                                   "ShuffleRead", op)

                start_task_execute = start_read + task.shuffle_read;

                write_duration(worker, start_task_execute, task.computing_duration(),
                               "Computing", op)

                start_shuffle_write = start_task_execute + task.computing_duration();
                if task.shuffle_write > 0:
                    write_duration(worker, start_shuffle_write, task.shuffle_write,
                                   "ShuffleWrite", op)

                write_duration(worker, task.task_result_serialize_start_ts + recv_ts - local_start,
                               task.task_result_serialize_end_ts - task.task_result_serialize_start_ts,
                               "Serialization", op)

                write_duration(worker, task.task_result_serialize2_start_ts + recv_ts - local_start,
                               task.task_result_serialize2_end_ts - task.task_result_serialize2_start_ts,
                               "Serialization", op)

                write_duration(worker, task.task_put_result_into_local_blockmanager_start_ts + recv_ts - local_start,
                               task.task_put_result_into_local_blockmanager_end_ts - task.task_put_result_into_local_blockmanager_start_ts,
                               "PuttingIntoBlockManager", op)

                # transformations
                if task.seq_op_start > 0:
                    write_duration(worker, task.seq_op_start + recv_ts - local_start,
                                   task.seq_op_end - task.seq_op_start, "SeqOp", op)

                if task.map_partition_with_index_start > 0:
                    write_duration(worker, task.map_partition_with_index_start + recv_ts - local_start,
                                   task.map_partition_with_index_end - task.map_partition_with_index_start,
                                   "MapPartitionWithIndex", op)

                if task.com_op_start > 0:
                    write_duration(worker, task.com_op_start + recv_ts - local_start,
                                   task.com_op_end - task.com_op_start, "CombOp", op)

                if task.input_map_start > 0:
                    write_duration(worker, task.input_map_start + recv_ts - local_start,
                                   task.input_map_end - task.input_map_start, "InputMap", op)

                if task.sample_filter_start > 0:
                    write_duration(worker, task.sample_filter_start + recv_ts - local_start,
                                   task.sample_filter_end - task.sample_filter_start, "Sample", op)

                # communication edge for sending back the result
                send_ts = task.task_put_result_into_local_blockmanager_end_ts + recv_ts - local_start
                # Before recv_ts is the driver time when executor starts to run.
                recv_ts = task.getting_result_ts.unwrap_or(task.finish_ts)
                if task.getting_result_time_ts == 0:
                    recv_ts = task.finish_ts
                else:
                    recv_ts = task.getting_result_time_ts
                write_control(worker, send_ts, DRIVER, recv_ts)
                # if we have it, emit a getting result activity at the driver

                if not task.getting_result_time_ts == 0:
                    write_duration(DRIVER, send_ts, task.getting_result_duration(), "GettingResult", op)


def write_duration(thread_id, start_ts, duration, eventType, operator):
    print "activityType:{}=start_ts:{}=duration:{}=workerId:{}=stageId:{}\n".format(eventType, start_ts, duration,
                                                                                    thread_id, operator)


def write_control(sender, send_ts, receiver, recv_ts):
    print "activityType:ControlMessage=sender:{}=send_ts:{}=receiver:{}=recv_ts:{}\n".format(sender, send_ts, receiver,
                                                                                             recv_ts)


def convert(inf, core_num, delay_split):
    ss = SparkState(delay_split, core_num)
    ss.read_json(inf)
    ss.write_logs()


if __name__ == "__main__":
    inf = "resultLogs"
    executor_cores = 8
    delay_split = 1
    convert(inf, executor_cores, delay_split)
