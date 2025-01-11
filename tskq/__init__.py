from __future__ import annotations
import asyncio
from uuid import uuid4, UUID
from dataclasses import dataclass
from typing import Coroutine, Any


class RunningError(Exception):
  ...


class StoppedError(Exception):
  ...


class TaskDoesNotExist(Exception):
  ...


class TaskQueue:

  Result = Any
  Task = Coroutine
  TaskID = UUID

  @dataclass
  class _TaskFrame:
    id: TaskQueue.TaskID
    task: Coroutine
    return_value: Any = None
    exception: Exception | None = None

  def __init__(self,*, interval: float = 0,retry: bool = False):
    '''
    Args:
      retry: 在发生异常时是否重试.
      interval: 任务间隔时间, 单位为秒.
      
    '''
    self._retry = retry
    self._interval = interval

    self._running = False
    self._stop_event = None

    self._running_queue: asyncio.Queue[TaskQueue.Task] = asyncio.Queue()
    self._tasks: dict[TaskQueue.Task, TaskQueue._TaskFrame | None] = {}

  @property
  def running(self):
    '''是否正在运行'''
    return self._running

  @property
  def _should_stop(self):
    '''是否应该停止'''
    # 如果指定了 stop_event，则通过 stop_event 判断是否应该停止
    if self._stop_event is not None:
      return self._stop_event.is_set()
    # 否则, 通过自身 running 状态判断是否应该停止
    return not self.running

  async def run(self, *, stop_event: asyncio.Event | None = None):
    '''
    运行任务队列
    Args:
      stop_event: 用于控制停止任务队列的事件, 由外部传入. asyncio.Event 对象. 如果传入了, 当 stop_event 被 set 时, 任务队列会尝试停止. 如果未传入, 则通过 TaskQueue.running 属性判断是否应该停止.
    '''
    if self.running:
      raise RunningError()
    self._stop_event = stop_event
    self._running = True
    await self._main_loop()
    self._running = False
    self._stop_event = None

  async def stop(self):
    '''停止任务队列'''
    if not self.running:
      raise StoppedError()
    self._running = False
    self._stop_event = None

  async def clear(self):
    # todo
    ...

  async def put(self, task: Task):
    '''
    添加任务到队列。
    Args:
      task (Task): 协程.
    Returns:
      UUID: 任务的唯一标识符，可用于后续获取任务结果.
    Raises:
      StoppedError: 当任务队列已停止时抛出.
    '''
    id = uuid4()
    self._tasks[id] = None
    frame = self._TaskFrame(id, task)
    await self._running_queue.put(frame)
    return id
  
  async def put_nowait(self, task: Task):
    '''
    添加任务到队列, 不等待.
    Args:
      task (Task): 协程.
    Raises:
      StoppedError: 当任务队列已停止时抛出.
    '''
    id = uuid4()
    frame = self._TaskFrame(id, task)
    await self._running_queue.put(frame)

  async def _main_loop(self):
    while not self._should_stop:
      frame = await self._running_queue.get()
      exp = None
      try:
        r = await frame.task
        frame.return_value = r
      except Exception as e:
        exp = frame.exception = e
      # 如果任务失败，且允许重试，则重新放入队列
      if exp and self._retry:
        await self._running_queue.put(frame)
      # 否则，任务完成
      else:
        self._tasks[frame.id] = frame
      self._running_queue.task_done()
      # 等待
      await asyncio.sleep(self._interval)

  async def get_result(self, id: TaskID) -> tuple[Result, Exception]:
    '''
    获取任务结果
    Args:
      id (UUID): 任务的唯一标识符.
    Returns:
      tuple[Result, Exception]: 任务的返回值和异常.
    Raises:
      TaskDoesNotExist: 当任务不存在时抛出.
    '''
    if id not in self._tasks:
      raise TaskDoesNotExist(id)
    frame = None
    while frame is None:
      await asyncio.sleep(0)
      frame = self._tasks[id]
    self._tasks.pop(id)
    return frame.return_value, frame.exception

  async def put_and_get(self, task: Task) -> tuple[Result, Exception]:
    '''
    添加任务到队列并获取结果
    Args:
      task (Task): 协程.
    Returns:
      tuple[Result, Exception]: 任务的返回值和异常.
    '''
    id = await self.put(task)
    return await self.get_result(id)
