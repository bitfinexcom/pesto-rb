module Pesto
  class Lock

    def initialize(ctx = {}, opts = {})
      @ctx = ctx

      raise 'ERR_REDIS_NOTFOUND' if @ctx[:pool].nil?

      @conf = {
        :timeout_lock_expire => 5,
        :timeout_lock => 1,
        :interval_check => 0.05
      }.merge(opts)

      load_scripts
    end

    def load_scripts
      cp.with do |rc|
        @script_sha = rc.script(
          :load,
          "local ret = 0 \
          local timeout = tonumber(ARGV[1]) \
          if type(timeout) ~= 'number' then \
            return 0 \
          end \
          local res = redis.call('setnx', KEYS[1], 1) \
          if res == 1 then \
            redis.call('expire', KEYS[1], ARGV[1]) \
            ret = 1 \
          else \
            ret = 0 \
          end \
          return ret"
        )
      end
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

    def lock _names, _opts = {}
      opts = merge_options _opts, :timeout_lock_expire, :timeout_lock, :interval_check

      names = (_names.is_a?(String) ? [_names] : _names).uniq
      opts[:timeout_lock_expire] = opts[:timeout_lock_expire].to_i
      opts[:timeout_lock_expire] += (opts[:timeout_lock] * names.size).ceil.to_i

      t_start = Time.now
      stop = false

      while true
        res, locks, stop = get_locks names, {
          :timeout_lock_expire => opts[:timeout_lock_expire]
        }

        break if stop || (Time.now - t_start) > opts[:timeout_lock]

        unlock locks
        sleep opts[:interval_check]
      end

      stop ? 1 : 0
    end

    def get_locks names, opts = {}
      locked = 0
      locks = []
      res = []

      timeout_lock_expire = opts[:timeout_lock_expire]

      cp.with do |rc|
        res = rc.multi do
          names.each do |n|
            res << rc.evalsha(@script_sha, {
              :keys => [lock_hash(n)],
              :argv => [timeout_lock_expire]
            })
          end
        end
      end

      names.each_with_index do |n, ix|
        next if res[ix] != 1
        locked += 1
        locks.push n
      end

      return [res, locks, locked == names.size]
    end

    def locki name = 'global', opts = {}
      lock name, opts.merge(timeout_lock: 0)
    end

    def lockx name = 'global', opts = {}, err = 'ERR_LOCKING'
      locked = lock(name, opts)
      return 1 if locked == 1

      raise "#{err} (#{name})"
    end

    def unlock _names = []
      _names = [_names] if _names.is_a?(String)
      names = _names.uniq
      res = []

      cp.with do |rc|
        res = rc.multi do
          names.each do |n|
            rc.del(lock_hash(n))
          end
        end
      end

      val = res.reduce(0){|sum, n| sum + n}

      val > 0 ? 1 : 0
    end

    private

    def lock_hash name
      "pesto:lock:#{name}"
    end

  end
end
