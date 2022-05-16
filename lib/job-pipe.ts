type Resolve = (data: any) => void
type Reject = (error: any) => void

type PipeCallback = (...args: any[]) => any

interface FlowItem {
  promise: Promise<any>
  reject: Reject
}

interface QueueItem {
  cb: PipeCallback
  args: any[]
  resolve: Resolve
  reject: Reject
}

export interface CreatePipeArgs {
  throughput?: number
  maxQueueSize?: number
}

export const createPipe = ({throughput = 1, maxQueueSize = 1} = {}) => {
  const flow: FlowItem[] = []
  const queue: QueueItem[] = []

  const getFlowWidth = () => flow.length

  const getQueueLength = () => queue.length

  const abort = () => {
    while (flow.length) {
      flow.shift().reject(new JobPipeAborted())
    }

    while (queue.length) {
      queue.shift().reject(new JobPipeAborted())
    }
  }

  const push = async (cb: PipeCallback, args: any[]) => {
    if (flow.length < throughput) {
      let reject: Reject

      const promise = new Promise((resolve, rj) => {
        reject = rj

        cb(...args)
          .then((data: any) => resolve(data))
          .catch((e: any) => rj(e))
      })

      const flowItem = {promise, reject}

      flow.push(flowItem)

      let success: any
      let error: any
      let hasError = false

      try {
        success = await promise
      } catch (e) {
        hasError = true
        error = e
      }

      flow.splice(
        flow.findIndex(item => item === flowItem),
        1,
      )

      setTimeout(() => {
        if (queue.length) {
          const next = queue.shift()
          push(next.cb, next.args)
            .then(data => {
              next.resolve(data)
            })
            .catch(e => {
              next.reject(e)
            })
        }
      })

      if (hasError) {
        throw error
      }

      return success
    } else {
      if (maxQueueSize <= 0) {
        return Promise.reject(new JobPipeQueueExceeded())
      }

      let resolve: Resolve
      let reject: Reject

      const promise = new Promise((rs, rj) => {
        resolve = rs
        reject = rj
      })

      const queueItem: QueueItem = {cb, resolve, reject, args}

      queue.push(queueItem)

      if (queue.length > maxQueueSize) {
        const failedQueueItem = queue.shift()
        failedQueueItem.reject(new JobPipeQueueExceeded())
      }

      return promise
    }
  }

  function getPipedCb<T extends PipeCallback>(cb: T): T {
    return ((...args: any[]) => push(cb, args)) as T
  }

  const result: typeof getPipedCb & {
    getFlowWidth: typeof getFlowWidth
    getQueueLength: typeof getQueueLength
    abort: typeof abort
  } = getPipedCb as any

  result.getFlowWidth = getFlowWidth
  result.getQueueLength = getQueueLength
  result.abort = abort

  return result
}

export const JOB_PIPE_ABORTED = 'JobPipeAborted'
export const JOB_PIPE_QUEUE_EXCEEDED_ERROR = 'JobPipeQueueExceeded'

export class JobPipeAborted extends Error {
  constructor() {
    super()
    this.name = JOB_PIPE_ABORTED
    this.message = 'Job pipe was aborted'
  }
}

export class JobPipeQueueExceeded extends Error {
  constructor() {
    super()
    this.name = JOB_PIPE_QUEUE_EXCEEDED_ERROR
    this.message = 'Job pipe queue was exceeded'
  }
}
