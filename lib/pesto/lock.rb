module Pesto
  class Lock

    def initialize(ctx = {}, opts = {})
      @ctx = ctx

      raise 'ERR_REDIS_NOTFOUND' if @ctx[:redis].nil?

      @conf = {
        :lock_expire => false,
        :timeout_lock_expire => 300,
        :timeout_lock => 90,
        :interval_check => 0.05,
        :concurrency_limit => 0,
        :concurrency_count => false
      }.merge(opts)
    end

    def conf
      @conf
    end

    def rc
      @ctx[:redis]
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
      locked = 0

      while true
        locked = 0

        res = rc.pipelined do
          names.each do |n|
            chash = lock_hash(n)
            rc.setnx chash, 1
          end
        end

        locks = []

        names.each_with_index do |n, ix|
          l = res[ix]
          next if !l
          locked += 1
          locks << n
        end

        if locked == names.size
          locked = 1

          if conf[:lock_expire]
            res = rc.pipelined do
              names.each do |n|
                chash = lock_hash(n)
                rc.expire chash, opts[:timeout_lock_expire]
              end
            end
          end
        else
          locked = 0
        end

        break if locked == 1 || (Time.now - t_start) > opts[:timeout_lock]

        unlock(locks)
        sleep opts[:interval_check]
      end

      locked == 0 ? 0 : 1
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

    def unlock(*names)
      names.uniq!

      res = rc.pipelined do
        names.each do |n|
          rc.del(lock_hash(n))
        end
      end

      val = 0
      res.each do |v|
        val += v
      end

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
