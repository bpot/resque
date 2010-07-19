module Resque
  # ThreadedResqueWorker
  class ThreadedWorker < Worker

    def work(interval = 5, &block)
      $0 = "resque: Starting"
      startup

      loop do
        break if @shutdown

        if not @paused and job = reserve
          log "got: #{job.inspect}"
          working_on job
          perform(job, &block)
          done_working
        else
          break if interval.to_i == 0
          log! "Sleeping for #{interval.to_i}"
          sleep interval.to_i
        end
      end

    ensure
      unregister_worker
    end

    # Processes a given job in the child.
    def perform(job)
      begin
        run_hook :after_fork, job
        job.perform
      rescue Object => e
        log "#{job.inspect} failed: #{e.inspect}"
        job.fail(e)
        failed!
      else
        log "done: #{job.inspect}"
      ensure
        yield job if block_given?
      end
    end

    # Returns a list of queues to use when searching for a job.
    # A splat ("*") means you want every queue (in alpha order) - this
    # can be useful for dynamically adding new queues.
    #
    # NB queues are randomized
    def queues
      @queues[0] == "*" ? Resque.queues.sort_by { rand } : @queues
    end

    # Runs all the methods needed when a worker begins its lifecycle.
    def startup
      register_worker

      # Fix buffering so we can `rake resque:work > resque.log` and
      # get output from the child in there.
      $stdout.sync = true
    end

    # Kill the child and shutdown immediately.
    def shutdown!
      shutdown
    end

    # The string representation is the same as the id for this worker
    # instance. Can be used with `Worker.find`.
    def to_s
      @to_s ||= "#{hostname}:#{Process.pid}:#{Thread.current.object_id}:#{@queues.join(',')}"
    end
    alias_method :id, :to_s
  end
end
