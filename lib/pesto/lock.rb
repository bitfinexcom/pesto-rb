module Pesto
  class Lock

    def initialize(ctx = {}, opts = {})
      @ctx = ctx

      raise 'ERR_REDIS_NOTFOUND' if @ctx[:pool].nil?

      @conf = {
        :lock_expire => true,
        :timeout_lock_expire => 5,
        :timeout_lock => 1,
        :interval_check => 0.05
      }.merge(opts)
    end

    def conf
      @conf
    end

    def cp
      @ctx[:pool]
    end

    def merge_options o = {}, *filter
      c = conf.merge(o)
      c.delete_if{|k,v| !filter.include?(k) } unless filter.empty?
      c
    end

    def lock(_names, _opts = {})
      opts = merge_options(_opts, :timeout_lock_expire, :timeout_lock, :interval_check, :concurrency_limit)

      names = (_names.is_a?(String) ? [_names] : _names).uniq
      opts[:timeout_lock_expire] = opts[:timeout_lock_expire].to_i
      opts[:timeout_lock_expire] += opts[:timeout_lock] * names.size

      t_start = Time.now

      while true
        res, locks, stop = get_locks names

        expire names, opts if !stop && conf[:lock_expire]

        break if stop || (Time.now - t_start) > opts[:timeout_lock]

        unlock(locks)
        sleep opts[:interval_check]
      end

      stop ? 1 : 0
    end

    def expire names, opts={}
      cp.with do |rc|
        rc.pipelined do
          names.each do |n|
            rc.expire lock_hash(n), opts[:timeout_lock_expire]
          end
        end
      end
    end

    def get_locks names
      locked = 0
      locks = []
      res = []

      cp.with do |rc|
        res = rc.pipelined do
          names.each do |n|
            rc.setnx lock_hash(n), 1
          end
        end

        names.each_with_index do |n, ix|
          next unless res[ix]
          locked += 1
          locks.push n
        end
      end

      return [res, locks, locked == names.size]
    end

    def locki(name = 'global', opts = {})
      lock name, opts.merge(timeout_lock: 0)
    end

    def lockx(name = 'global', opts = {}, err = 'ERR_LOCKING')
      locked = lock(name, opts)
      return 1 if locked == 1

      raise "#{err} (#{name})"
    end

    def lockm(_names = [], opts = {})
      lock(_names, opts)
    end

    def unlock(_names = [])
      _names = [_names] if _names.is_a?(String)
      names = _names.uniq
      res = []

      cp.with do |rc|
        res = rc.pipelined do
          names.each do |n|
            rc.del(lock_hash(n))
          end
        end
      end

      val = res.reduce(0){|sum,n| sum+n}

      val > 0 ? 1 : 0
    end

    def unlockm(_names)
      unlock(_names)
    end

    private

    def lock_hash(name)
      "pesto:lock:#{name}"
    end

  end
end
