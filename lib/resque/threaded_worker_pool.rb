module Resque
  class ThreadedWorkerPool
    def initialize(queues, pool_size = 10)
      @queues     = queues
      @pool_size  = pool_size
      @threads    = []
      @workers    = []
    end

    def work(interval)
      startup 

      @pool_size.times do
        worker = ThreadedWorker.new(*@queues)

        @workers << worker
        @threads << Thread.new do
          worker.work(interval)
        end
      end

      @threads.each { |w| w.join }
    end

    def startup
      register_signal_handlers
      prune_dead_workers
    end

    def register_signal_handlers
      trap('TERM') { @workers.each { |w| w.shutdown } }
      trap('INT')  { @workers.each { |w| w.shutdown } }

      begin
        trap('QUIT') { @workers.each { |w| w.shutdown } }
        #trap('USR1') { kill_child }
        trap('USR2') { @workers.each { |w| w.pause_processing } }
        trap('CONT') { @workers.each { |w| w.unpause_processing } }
      rescue ArgumentError
        warn "Signals QUIT, USR1, USR2, and/or CONT not supported."
      end

#      log! "Registered signals"
    end

    def prune_dead_workers
      all_workers = Worker.all
      known_workers = worker_pids unless all_workers.empty?
      all_workers.each do |worker|
        host, pid, thread_id, queues = worker.id.split(':')
        next unless host == hostname
        next if known_workers.include?(pid)
        log! "Pruning dead worker: #{worker}"
        worker.unregister_worker
      end
    end

    # Returns an array of string pids of all the other workers on this
    # machine. Useful when pruning dead workers on startup.
    def worker_pids
      `ps -A -o pid,command | grep [r]esque`.split("\n").map do |line|
        line.split(' ')[0]
      end
    end

    # chomp'd hostname of this machine
    def hostname
      @hostname ||= `hostname`.chomp
    end
  end
end
