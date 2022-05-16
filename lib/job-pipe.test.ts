import {createPipe, JobPipeQueueExceeded, JobPipeAborted} from './job-pipe'

describe('Async pipe', () => {
  it('executes a job', async () => {
    const pipe = createPipe()

    let resolve

    const promise = new Promise(r => (resolve = r))
    const fn = jest.fn()

    const pipePromise = pipe(() => promise)().then(fn)
    expect(fn).not.toHaveBeenCalled()

    resolve()
    await pipePromise

    expect(fn).toHaveBeenCalledTimes(1)
  })

  it('takes argument and resolves a promise', async () => {
    const arg = 'TEST'

    const pipe = createPipe()
    const actual = await pipe(async a => a)(arg)

    expect(actual).toBe(arg)
  })

  it('rejects job promise', async () => {
    const REJECTION_VALUE = 'test'

    const pipe = createPipe()

    let actual: string

    try {
      await pipe(() => Promise.reject(REJECTION_VALUE))()
    } catch (e) {
      actual = e
    }

    expect(actual).toBe(REJECTION_VALUE)
  })

  it('executes multiple jobs at the same time if pipe is wide enough', () => {
    const pipe = createPipe({throughput: 2})

    const promise1 = new Promise(() => {})
    const promise2 = new Promise(() => {})

    const fn1 = jest.fn(() => promise1)
    const fn2 = jest.fn(() => promise2)

    pipe(fn1)()
    pipe(fn2)()

    expect(fn1).toHaveBeenCalledTimes(1)
    expect(fn2).toHaveBeenCalledTimes(1)
  })

  it('delays job execution if pipe is not wide enough', async () => {
    const pipe = createPipe({throughput: 2})

    let resolve

    const promise1 = new Promise(r => (resolve = r))
    const promise2 = new Promise(() => {})
    const promise3 = new Promise(() => {})

    const fn1 = jest.fn(() => promise1)
    const fn2 = jest.fn(() => promise2)
    const fn3 = jest.fn(() => promise3)

    const pipePromise = pipe(fn1)()
    pipe(fn2)()
    pipe(fn3)()

    expect(fn1).toHaveBeenCalledTimes(1)
    expect(fn2).toHaveBeenCalledTimes(1)
    expect(fn3).not.toHaveBeenCalled()

    resolve()
    await pipePromise
    jest.runOnlyPendingTimers()

    expect(fn1).toHaveBeenCalledTimes(1)
    expect(fn2).toHaveBeenCalledTimes(1)
    expect(fn3).toHaveBeenCalledTimes(1)
  })

  it('cancels job if there is no waitlist', async () => {
    const pipe = createPipe({throughput: 2, maxQueueSize: 0})

    let resolve

    const promise1 = new Promise(r => (resolve = r))
    const promise2 = new Promise(() => {})
    const promise3 = new Promise(() => {})

    const fn1 = jest.fn(() => promise1)
    const fn2 = jest.fn(() => promise2)
    const fn3 = jest.fn(() => promise3)

    const pipePromise = pipe(fn1)()
    pipe(fn2)()

    let error

    try {
      await pipe(fn3)()
    } catch (e) {
      error = e
    }

    expect(error).toBeInstanceOf(JobPipeQueueExceeded)

    resolve()
    await pipePromise

    expect(fn1).toHaveBeenCalledTimes(1)
    expect(fn2).toHaveBeenCalledTimes(1)
    expect(fn3).not.toHaveBeenCalled()
  })

  it('cancels older jobs if waitlist is not sufficient', async () => {
    const pipe = createPipe({throughput: 1, maxQueueSize: 1})

    let resolve

    const promise1 = new Promise(r => (resolve = r))
    const promise2 = new Promise(() => {})
    const promise3 = new Promise(() => {})

    const fn1 = jest.fn(() => promise1)
    const fn2 = jest.fn(() => promise2)
    const fn3 = jest.fn(() => promise3)

    const pipePromise = pipe(fn1)()

    let error

    const middlePromise = (async () => {
      try {
        await pipe(fn2)()
      } catch (e) {
        error = e
      }
    })()

    pipe(fn3)()

    await middlePromise

    expect(error).toBeInstanceOf(JobPipeQueueExceeded)

    expect(fn1).toHaveBeenCalledTimes(1)
    expect(fn2).not.toHaveBeenCalled()
    expect(fn3).not.toHaveBeenCalled()

    resolve()
    await pipePromise
    jest.runOnlyPendingTimers()

    expect(fn1).toHaveBeenCalledTimes(1)
    expect(fn2).not.toHaveBeenCalled()
    expect(fn3).toHaveBeenCalledTimes(1)
  })

  it('does not interfere with multiple job execution if pipe remains not filled', () => {
    const pipe = createPipe({throughput: 4})

    const promise1 = new Promise(() => {})
    const promise2 = new Promise(() => {})
    const promise3 = new Promise(() => {})
    const promise4 = new Promise(() => {})

    const fn1 = jest.fn(() => promise1)
    const fn2 = jest.fn(() => promise2)
    const fn3 = jest.fn(() => promise3)
    const fn4 = jest.fn(() => promise4)

    pipe(fn1)()
    pipe(fn2)()
    pipe(fn3)()
    pipe(fn4)()

    expect(fn1).toHaveBeenCalledTimes(1)
    expect(fn2).toHaveBeenCalledTimes(1)
    expect(fn3).toHaveBeenCalledTimes(1)
    expect(fn4).toHaveBeenCalledTimes(1)
  })

  it('provides current flow width', () => {
    const pipe = createPipe({throughput: 4})

    const promise1 = new Promise(() => {})
    const promise2 = new Promise(() => {})
    const promise3 = new Promise(() => {})

    const fn1 = jest.fn(() => promise1)
    const fn2 = jest.fn(() => promise2)
    const fn3 = jest.fn(() => promise3)

    pipe(fn1)()
    pipe(fn2)()
    pipe(fn3)()

    expect(pipe.getFlowWidth()).toBe(3)
  })

  it('provides current queue length', () => {
    const pipe = createPipe({throughput: 1, maxQueueSize: 4})

    const promise1 = new Promise(() => {})
    const promise2 = new Promise(() => {})
    const promise3 = new Promise(() => {})
    const promise4 = new Promise(() => {})

    const fn1 = jest.fn(() => promise1)
    const fn2 = jest.fn(() => promise2)
    const fn3 = jest.fn(() => promise3)
    const fn4 = jest.fn(() => promise4)

    pipe(fn1)()
    pipe(fn2)()
    pipe(fn3)()
    pipe(fn4)()

    expect(pipe.getQueueLength()).toBe(3)
  })

  it('allows rejecting all jobs', async () => {
    const pipe = createPipe({throughput: 1, maxQueueSize: 4})

    const promise1 = new Promise(() => {})
    const promise2 = new Promise(() => {})
    const promise3 = new Promise(() => {})
    const promise4 = new Promise(() => {})

    const fn1 = jest.fn(() => promise1)
    const fn2 = jest.fn(() => promise2)
    const fn3 = jest.fn(() => promise3)
    const fn4 = jest.fn(() => promise4)

    const pipePromise1 = pipe(fn1)()
    const pipePromise2 = pipe(fn2)()
    const pipePromise3 = pipe(fn3)()
    const pipePromise4 = pipe(fn4)()

    pipe.abort()

    let error1
    let error2
    let error3
    let error4

    try {
      await pipePromise1
    } catch (e) {
      error1 = e
    }

    try {
      await pipePromise2
    } catch (e) {
      error2 = e
    }

    try {
      await pipePromise3
    } catch (e) {
      error3 = e
    }

    try {
      await pipePromise4
    } catch (e) {
      error4 = e
    }

    expect(error1).toBeInstanceOf(JobPipeAborted)
    expect(error2).toBeInstanceOf(JobPipeAborted)
    expect(error3).toBeInstanceOf(JobPipeAborted)
    expect(error4).toBeInstanceOf(JobPipeAborted)
    expect(pipe.getFlowWidth()).toBe(0)
    expect(pipe.getQueueLength()).toBe(0)
  })

  it('allows aborting pipe properly from a failed pipe job', async () => {
    const pipe = createPipe({throughput: 1, maxQueueSize: 1})

    let reject

    const promise1 = new Promise((_, rj) => {
      reject = rj
    })

    let resolve

    const promise2 = new Promise(rs => {
      resolve = rs
    })

    const fn1 = jest.fn(() => promise1)
    const fn2 = jest.fn(() => promise2)

    pipe(fn1)().catch(() => {
      pipe.abort()
    })

    const pipePromise2 = pipe(fn2)()

    reject()
    resolve()

    let error = false

    try {
      await pipePromise2
    } catch (e) {
      error = true
    }

    expect(error).toBeTruthy()
    expect(fn2).not.toHaveBeenCalled()
  })
})
