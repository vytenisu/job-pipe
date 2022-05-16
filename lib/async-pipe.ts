type Resolve = (data: any) => void;
type Reject = (error: any) => void;

type PipeCallback = (...args: any[]) => any;

interface FlowItem {
  promise: Promise<any>;
  reject: Reject;
}

interface QueueItem {
  cb: PipeCallback;
  args: any[];
  resolve: Resolve;
  reject: Reject;
}

export interface CreateAsyncPipeArgs {
  throughput?: number;
  maxQueueSize?: number;
}

export const createAsyncPipe = ({ throughput = 1, maxQueueSize = 1 } = {}) => {
  const flow: FlowItem[] = [];
  const queue: QueueItem[] = [];

  const getFlowWidth = () => flow.length;

  const getQueueLength = () => queue.length;

  const abort = () => {
    while (flow.length) {
      flow.shift().reject(new AsyncPipeAborted());
    }

    while (queue.length) {
      queue.shift().reject(new AsyncPipeAborted());
    }
  };

  const push = async (cb: PipeCallback, args: any[]) => {
    if (flow.length < throughput) {
      let reject: Reject;

      const promise = new Promise((resolve, rj) => {
        reject = rj;

        cb(...args)
          .then(data => resolve(data))
          .catch(e => rj(e));
      });

      const flowItem = { promise, reject };

      flow.push(flowItem);

      let success: any;
      let error: any;
      let hasError = false;

      try {
        success = await promise;
      } catch (e) {
        hasError = true;
        error = e;
      }

      flow.splice(
        flow.findIndex(item => item === flowItem),
        1,
      );

      setTimeout(() => {
        if (queue.length) {
          const next = queue.shift();
          push(next.cb, next.args)
            .then(data => {
              next.resolve(data);
            })
            .catch(e => {
              next.reject(e);
            });
        }
      });

      if (hasError) {
        throw error;
      }

      return success;
    } else {
      if (maxQueueSize <= 0) {
        return Promise.reject(new AsyncPipeQueueExceeded());
      }

      let resolve: Resolve;
      let reject: Reject;

      const promise = new Promise((rs, rj) => {
        resolve = rs;
        reject = rj;
      });

      const queueItem: QueueItem = { cb, resolve, reject, args };

      queue.push(queueItem);

      if (queue.length > maxQueueSize) {
        const failedQueueItem = queue.shift();
        failedQueueItem.reject(new AsyncPipeQueueExceeded());
      }

      return promise;
    }
  };

  function getPipedCb<T extends PipeCallback>(cb: T): T {
    return ((...args: any[]) => push(cb, args)) as T;
  }

  const result: typeof getPipedCb & {
    getFlowWidth: typeof getFlowWidth;
    getQueueLength: typeof getQueueLength;
    abort: typeof abort;
  } = getPipedCb as any;

  result.getFlowWidth = getFlowWidth;
  result.getQueueLength = getQueueLength;
  result.abort = abort;

  return result;
};

export const ASYNC_PIPE_ABORTED = 'AsyncPipeAborted';
export const ASYNC_PIPE_QUEUE_EXCEEDED_ERROR = 'AsyncPipeQueueExceeded';

export class AsyncPipeAborted extends Error {
  constructor() {
    super();
    this.name = ASYNC_PIPE_ABORTED;
    this.message = 'Async pipe was aborted';
  }
}

export class AsyncPipeQueueExceeded extends Error {
  constructor() {
    super();
    this.name = ASYNC_PIPE_QUEUE_EXCEEDED_ERROR;
    this.message = 'Async pipe queue was exceeded';
  }
}
